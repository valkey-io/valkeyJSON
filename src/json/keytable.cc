#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <sstream>
#include <mutex>
#include <unordered_map>
#include <cstring>
#include <iostream>

extern "C" {
#include <./include/valkeymodule.h>
}

#define KEYTABLE_ASSERT ValkeyModule_Assert
#include <json/keytable.h>

/***************************************************************************************************
 *
 * The shard implements a hashtable of entries. Each entry consists of a pointer to a unique string.
 *
 * We implement open addressing using linear probing (see https://en.wikipedia.org/wiki/Linear_probing)
 * See the text for details of the search, insert and deletion algorithms.
 *
 * A hash table is a vector of pointers to KeyTable_Layout objects. Thus for insertion, searching
 * and deletion only a single hashing of the key passed in as a parameter is required, no subsequent
 * hash computations are done.
 *
 * The rehash algorithm needs the hash value for each key in order to insert it into the hash table.
 *  For simplicity, rehashing is done synchronously.
 *
 * If the new table size is less than 2^19, we store the low order 19 bits of the original hash value in
 * the hash-table entry itself (since it's a pointer) and just use that. Thus the only memory associated
 * with the new and old hash tables needs to be accessed, reducing the cache footprint.
 *
 * If the new table size is greater than 2^19, we're out of bits in the pointer so we have to use the
 * full value of the hash. That hash value is not recomputed, because we saved it when we generated
 * the entry originally (see KeyTable_Layout), so it's just fetched. But fetching of the hash value will
 * cause an extra cache miss, further increasing the cost of a rehash for large tables.
 *
 * There's a trade-off between the number of shards and the size of each shard hashtable. We really want
 * to keep the shard hashtable below 2^19 so that rehashes are fast. Thus when a table size grows to
 * be larger than 2^19, we put out a warning into the log that performance would suffer and we should
 * increase the number of shards. Someday we could hook this up to an alarm.
 *
 */

// non-constant so unit tests can control it
size_t MAX_FAST_TABLE_SIZE = PtrWithMetaData<KeyTable_Layout>::METADATA_MASK + 1;

struct KeyTable_Shard {
    typedef PtrWithMetaData<KeyTable_Layout> EntryType;
    size_t capacity;                        // Number of entries in table
    size_t size;                            // Number of current entries
    size_t bytes;                           // number of bytes of all current entries
    size_t handles;                         // number of handles outstanding
    size_t maxSearch;                       // Max length of a search, since last read
    EntryType *entries;                     // Array of String Entries
    std::mutex mutex;                       // lock for this shard, mutable for "validate"
    uint32_t rehashes;                      // number of rehashes, since last read
    static constexpr size_t MIN_TABLE_SIZE = 4;

    //
    // This logic implements the optimization that for Fast tables, we just get the low 19-bits of
    // the original hash value. Thereby avoiding an extra cache hit to fetch it from the Key itself
    //
    size_t getHashValueFromEntry(const EntryType& e) const {
        size_t hashValue;
        if (capacity < MAX_FAST_TABLE_SIZE) {
            hashValue = e.getMetaData();
        } else {
            hashValue = e->getOriginalHash();
            KEYTABLE_ASSERT((hashValue & EntryType::METADATA_MASK) == e.getMetaData());
        }
        return hashValue;
    }

    void makeTable(const KeyTable& t, size_t newCapacity) {
        newCapacity = std::max(newCapacity, MIN_TABLE_SIZE);
        KEYTABLE_ASSERT(newCapacity != capacity);  // oops full or empty.
        capacity = newCapacity;
        entries = new (t.malloc(capacity * sizeof(EntryType))) EntryType[capacity];
    }

    KeyTable_Shard() : mutex() {
        capacity = 0;
        size = 0;
        bytes = 0;
        handles = 0;
        entries = nullptr;
        rehashes = 0;
        maxSearch = 0;
    }

    //
    // The only real use for this is in the unit tests.
    // We scan the table to make sure all keys are gone.
    //
    void destroy(KeyTable& t) {
        MEMORY_VALIDATE(entries);
        for (size_t i = 0; i < capacity; ++i) {
            if (entries[i]) {
                KEYTABLE_ASSERT(entries[i]->IsStuck());
                t.free(&*entries[i]);
            }
            entries[i].clear();
        }
        t.free(entries);
        entries = nullptr;
    }

    ~KeyTable_Shard() {
        KEYTABLE_ASSERT(entries == nullptr);
    }

    float loadFactor() { return float(size) / float(capacity); }

    size_t hashIndex(size_t hash) const { return hash % capacity; }

    KeyTable_Layout *insert(KeyTable& t, size_t hsh, const char *ptr, size_t len, bool noescape) {
        std::scoped_lock lck(mutex);
        while (loadFactor() > t.getFactors().maxLoad) {
            //
            // Oops, table too full, resize it larger.
            //
            size_t newSize = capacity + std::max(size_t(capacity * t.getFactors().grow), size_t(1));
            resizeTable(t, newSize);
            if (newSize >= MAX_FAST_TABLE_SIZE) {
                ValkeyModule_Log(nullptr, "warning",
                                "Fast KeyTable Shard size exceeded, increase "
                                "json.key-table-num-shards to improve performance");
            }
        }
        size_t ix = hashIndex(hsh);
        size_t metadata = hsh & EntryType::METADATA_MASK;
        MEMORY_VALIDATE(entries);
        for (size_t searches = 0; searches < capacity; ++searches) {
            EntryType &entry = entries[ix];
            if (!entry) {
                //
                // Empty, insert it here.
                //
                handles++;
                size++;
                bytes += len;
                maxSearch = std::max(searches, maxSearch);

                KeyTable_Layout *p = KeyTable_Layout::makeLayout(t.malloc, ptr, len, hsh, noescape);
                entry = EntryType(p, metadata);
                return p;
            } else if (entry.getMetaData() == metadata &&    // Early out, don't hit the cache line....
                        len == entry->getLength() &&
                        0 == std::memcmp(ptr, entry->getText(), len)) {
                //
                // easy case. String already present, just bump the refcount and we're done.
                // Use saturating arithmetic so it never fails. If you manage to legitimately
                // have a reuse count > 2^29 then you'll never be able to recover the memory
                // from that string. But who cares?
                //
                maxSearch = std::max(searches, maxSearch);
                handles++;
                if (entry->incrRefCount()) {
                    t.stuckKeys++;
                }
                return &*entry;
            }
            if (++ix >= capacity) {
                ix = 0;
            }
        }
        KEYTABLE_ASSERT(false);
        return nullptr;
    }

    //
    // forward distance is defined as the number of increments (wrapping around) it takes to go
    // from "from" to "to". Accounting for wrap-around
    //
    size_t forward_distance(size_t from, size_t to) {
        size_t result;
        if (from <= to) {
            result = to - from;
        } else {
            result = (to + capacity) - from;
        }
        KEYTABLE_ASSERT(result < capacity);
        return result;
    }

    KeyTable_Layout *clone(KeyTable& t, const KeyTable_Handle& h) {
        std::scoped_lock lck(mutex);
        handles++;
        if (h->incrRefCount()) {
            t.stuckKeys++;
        }
        return const_cast<KeyTable_Layout *>(&*h);
    }

    void destroyHandle(const KeyTable& t, KeyTable_Handle& h, size_t hsh) {
        std::scoped_lock lck(mutex);
        handles--;
        if (h->decrRefCount() > 0) {
            h.clear();  // Kill the handle
            return;  // Easy case, still referenced.
        }
        //
        // Ok, we need to remove this string from the hashtable.
        //
        size_t ix = hashIndex(hsh);
        MEMORY_VALIDATE(entries);
        for (size_t searches = 0; searches < capacity; ++searches) {
            if (&*entries[ix] == &*h) {
                //
                // Found it!!!
                // Update stats, nuke the handle and recover the space.
                //
                KEYTABLE_ASSERT(entries[ix].getMetaData() == (hsh & EntryType::METADATA_MASK));
                KEYTABLE_ASSERT(entries[ix]->getRefCount() == 0);
                KEYTABLE_ASSERT(size > 0);
                KEYTABLE_ASSERT(bytes >= h->getLength());
                bytes -= h->getLength();
                size--;
                h.theHandle->poisonOriginalHash();
                t.free(&*h.theHandle);
                h.clear();        // Kill the handle
                entries[ix].clear();
                //
                // Now reestablish the invariant of the algorithm by scanning forward until
                // we hit another empty cell. While we're scanning we may have to move keys down
                // into the newly freed slot.
                //
                size_t empty_ix = ix;  // Remember where the empty slot is.
                if (++ix >= capacity) ix = 0;  // Next entry
                while (entries[ix]) {
                    KEYTABLE_ASSERT(!entries[empty_ix]);
                    KEYTABLE_ASSERT(empty_ix != ix);
                    searches++;
                    //
                    // This non-empty key might have to be moved down to the empty slot.
                    // That happens if the forward_distance of the empty slot is less than
                    // The forward_distance of the current slot to the native slot for this key.
                    //
                    size_t nativeSlot = hashIndex(getHashValueFromEntry(entries[ix]));
                    if (forward_distance(nativeSlot, ix) > forward_distance(nativeSlot, empty_ix)) {
                        // Yes, this key can be moved.
                        entries[empty_ix] = entries[ix];
                        entries[ix].clear();
                        empty_ix = ix;
                    }
                    if (++ix >= capacity) ix = 0;
                }
                maxSearch = std::max(searches, maxSearch);
                //
                // Having removed an entry, check for rehashing
                //
                if (loadFactor() < t.getFactors().minLoad && capacity > MIN_TABLE_SIZE) {
                    size_t reduction = std::max(size_t(capacity * t.getFactors().shrink), size_t(1));
                    resizeTable(t, capacity - reduction);
                }
                return;
            }
            if (++ix >= capacity) {
                ix = 0;
            }
        }
        KEYTABLE_ASSERT(false);  // Not found ????
    }

    void resizeTable(const KeyTable& t, size_t newSize) {
        uint64_t startTime = ValkeyModule_Milliseconds();
        if (capacity == newSize) return;      // Nothing to do.
        KEYTABLE_ASSERT(newSize >= size);     // Otherwise it won't fit.
        rehashes++;
        MEMORY_VALIDATE(entries);
        EntryType *oldEntries = entries;
        size_t oldCapacity = capacity;
        makeTable(t, newSize);
        for (size_t i = 0; i < oldCapacity; ++i) {
            if (oldEntries[i]) {
                //
                // Found valid entry, Compute hash to see where it goes.
                //
                EntryType& oldEntry = oldEntries[i];
                KEYTABLE_ASSERT(oldEntry->getRefCount() > 0);
                size_t ix = hashIndex(getHashValueFromEntry(oldEntry));
                for (size_t searches = 0; searches < capacity; ++searches) {
                    if (!entries[ix]) {
                        //
                        // Empty, insert it.
                        //
                        entries[ix] = oldEntry;
                        maxSearch = std::max(searches, maxSearch);
                        goto nextOldEntry;
                    }
                    if (++ix >= capacity) ix = 0;
                }
                KEYTABLE_ASSERT(false);  // can't fail if
            }
        nextOldEntry:{}
        }
        t.free(oldEntries);
        uint64_t duration = ValkeyModule_Milliseconds() - startTime;
        if (duration == 0) duration = 1;
        uint64_t keys_per_second = (size / duration) * 1000;
        ValkeyModule_Log(nullptr, "notice",
                        "Keytable Resize to %zu completed in %lu ms (%lu / sec)",
                        capacity, duration, keys_per_second);
    }

    //
    // Validate all of the entries and the counters. Unit test stuff.
    //
    std::string validate(const KeyTable& t, size_t shardNumber) const {
        std::scoped_lock lck(const_cast<std::mutex&>(this->mutex));  // Cheat on the mutex
        size_t this_refs  = 0;
        size_t this_size  = 0;
        size_t this_bytes = 0;
        for (size_t i = 0; i < capacity; ++i) {
            EntryType e = entries[i];
            if (e) {
                this_size++;
                this_refs += e->getRefCount();
                this_bytes += e->getLength();
                size_t orig_hash = t.hash(e->getText(), e->getLength());
                size_t correct_metadata = orig_hash & EntryType::METADATA_MASK;
                size_t nativeIx = hashIndex(orig_hash);
                // Validate the metadata field
                if (e.getMetaData() != correct_metadata) {
                    std::ostringstream os;
                    os << "Found bad metadata in slot " << i << " Metadata:" << e.getMetaData()
                       << " Where it should be: " << correct_metadata << " Hash:" << orig_hash
                       << " TableSize:" << capacity;
                    return os.str();
                }
                //
                // Check the Invariant. If this entry isn't in its original slot (hashIndex) then
                // none of the locations between the original slot and this one may be empty.
                //
                for (size_t ix = nativeIx; ix != i;) {
                    if (!entries[ix]) {
                        // Error
                        std::ostringstream os;
                        os << "Found invalid empty location at slot " << ix << " While validating"
                           << " key in slot " << i << " From NativeSlot:" << nativeIx
                           << " TableSize:" << capacity;
                        return os.str();
                    }
                    if (++ix >= capacity) ix = 0;
                }
            }
        }
        // compare the counts. The summed refcounts only match handle counts if no stuck strings
        if (this_size != size ||
            (t.stuckKeys == 0 ? this_refs != handles : false) ||
            this_bytes != bytes) {
            std::ostringstream os;
            os << "Count mismatch for shard: " << shardNumber << " Capacity:" << capacity
                << " Handles:" << handles << " sum(refcounts):" << this_refs
                << " Size:" << size << " this_size:" << this_size
                << " Bytes:" << bytes << " this_bytes:" << this_bytes;
            return os.str();
        }
        return std::string();  // Empty means no failure.
    }
    std::string validate_counts(std::unordered_map<const KeyTable_Layout *, size_t>& counts) const {
        std::string result;
        std::scoped_lock lck(const_cast<std::mutex&>(this->mutex));  // Cheat on the mutex
        for (size_t i = 0; i < capacity; ++i) {
            EntryType e = entries[i];
            if (e) {
                if (counts[&*e] != e->getRefCount()) {
                    std::ostringstream os;
                    os
                        << "Found bad count for key: " << e->getText()
                        << " Found: " << e->getRefCount()
                        << " Expected:" << counts[&*e]
                        << "\n";
                    result += os.str();
                } else {
                    counts.erase(&*e);
                }
            }
        }
        return result;  // Empty means no failures found.
    }
    // Add our stats to the total so far.
    void updateStats(KeyTable::Stats& s) {
        std::scoped_lock lck(mutex);
        s.size += size;
        s.bytes += bytes;
        s.handles += handles;
        s.maxTableSize = std::max(s.maxTableSize, capacity);
        s.minTableSize = std::min(s.minTableSize, capacity);
        s.totalTable += capacity;
        s.rehashes += rehashes;
        s.maxSearch = std::max(s.maxSearch, maxSearch);
        //
        // Reset the counters
        //
        maxSearch = 0;
        rehashes = 0;
    }
    // Add our stats
    void updateLongStats(KeyTable::LongStats& s, size_t topN) {
        std::scoped_lock lck(mutex);
        size_t thisRun = 0;
        for (size_t i = 0; i < capacity; ++i) {
            if (entries[i]) {
                thisRun++;
            } else if (thisRun != 0) {
                s.runs[thisRun]++;
                while (s.runs.size() > topN) {
                    s.runs.erase(s.runs.begin());
                }
                thisRun = 0;
            }
        }
    }
};

/*
 * Setup the KeyTable itself.
 */
KeyTable::KeyTable(const Config& cfg) :
    malloc(cfg.malloc),
    free(cfg.free),
    hash(cfg.hash),
    numShards(cfg.numShards),
    stuckKeys(0)
{
    KEYTABLE_ASSERT(numShards > 0);
    KEYTABLE_ASSERT(malloc && free && hash);
    shards = new(malloc(numShards * sizeof(KeyTable_Shard))) KeyTable_Shard[numShards];
    for (size_t i = 0; i < numShards; ++i) shards[i].makeTable(*this, 1);
    KEYTABLE_ASSERT(!isValidFactors(factors));
}

KeyTable::~KeyTable() {
    MEMORY_VALIDATE(shards);
    for (size_t i = 0; i < numShards; ++i) {
        shards[i].destroy(*this);
        shards[i].~KeyTable_Shard();
    }
    free(shards);
    shards = nullptr;
}

std::string KeyTable::validate() const {
    std::string s;
    for (size_t i = 0; i < numShards; ++i) {
        s += shards[i].validate(*this, i);
    }
    return s;
}

std::string KeyTable::validate_counts(std::unordered_map<const KeyTable_Layout *, size_t>& counts) const {
    std::string result;
    result = validate();
    if (!result.empty()) return result;
    //
    // Now, we need to double compare the current keytable against the counts array.
    // We scan the KeyTables and lookup each handle as we go, erasing it from the input counts map.
    // If at the end, there are any counts entries left, then we definitely have a problem.
    //
    for (size_t i = 0; i < numShards; ++i) {
        result += shards[i].validate_counts(counts);
    }
    if (!result.empty()) return result;
    //
    // Now validate we found everything
    //
    if (!counts.empty()) {
        for (auto& c : counts) {
            std::ostringstream os;
            os << "Lingering Handle found: " << c.first->getText() << " Count:" << c.second << "\n";
            result += os.str();
        }
    }
    return result;
}

KeyTable::Stats KeyTable::getStats() const {

    Stats s{};

    //
    // Global stats
    //
    s.stuckKeys = stuckKeys;
    s.factors = getFactors();
    //
    // Now sum up the per-shard stats
    //
    for (size_t i = 0; i < numShards; ++i) {
        shards[i].updateStats(s);
    }
    return s;
}

KeyTable::LongStats KeyTable::getLongStats(size_t topN) const {
    LongStats s;
    //
    // Now sum up the per-shard stats
    //
    for (size_t i = 0; i < numShards; ++i) {
        shards[i].updateLongStats(s, topN);
    }
    return s;
}

/*
 * Take 19 bits from hash, avoid the low end of the hash value as this is used for the per-shard index.
 */
size_t KeyTable::shardNumberFromHash(size_t hash) {
    return (hash >> 40) % numShards;
}

size_t KeyTable::hashcodeFromHash(size_t hash) {
    return hash & KeyTable_Handle::MAX_HASHCODE;
}

/*
 * Upsert a string, returns a handle for this insertion.
 *
 * This function hashes the string and dispatches the operation to the appropriate shard.
 */
KeyTable_Handle KeyTable::makeHandle(const char *ptr, size_t len, bool noescape) {
    size_t hsh = hash(ptr, len);
    size_t shardNum = shardNumberFromHash(hsh);
    KeyTable_Layout *s = shards[shardNum].insert(*this, hsh, ptr, len, noescape);
    return KeyTable_Handle(s, hashcodeFromHash(hsh));
}

/*
 * Clone an existing handle
 */
KeyTable_Handle KeyTable::clone(const KeyTable_Handle& h) {
    size_t hsh = hash(h.GetString(), h.GetStringLength());
    size_t shardNum = shardNumberFromHash(hsh);
    KeyTable_Layout *s = shards[shardNum].clone(*this, h);
    return KeyTable_Handle(s, hashcodeFromHash(hsh));
}

/*
 * Destroy a Handle.
 *
 * While technically, we don't have to hash the string to determine the shard, the shard-level
 * destroy operation will need the hash, so we do it here for symmetry.
 */
void KeyTable::destroyHandle(KeyTable_Handle& h) {
    if (!h) return;  // Empty
    size_t hsh = hash(h.GetString(), h.GetStringLength());
    KEYTABLE_ASSERT(!h->isPoisoned());
    KEYTABLE_ASSERT(hsh == h->getOriginalHash());
    size_t shardNum = shardNumberFromHash(hsh);
    shards[shardNum].destroyHandle(*this, h, hsh);
}

void KeyTable::setFactors(const Factors& f) {
    KEYTABLE_ASSERT(!isValidFactors(f));
    // Grab all of the locks to ensure consistency
    for (size_t i = 0; i < numShards; ++i) {
        shards[i].mutex.lock();
    }
    factors = f;
    for (size_t i = 0; i < numShards; ++i) {
        shards[i].mutex.unlock();
    }
}

const char *KeyTable::isValidFactors(const Factors& f) {
    //
    // first the easy ones....
    //
    if (f.minLoad <= 0) return "minLoad <= 0.0";
    if (f.maxLoad > 1.0f) return "maxLoad > 1.0";
    if (f.minLoad >= f.maxLoad) return "minLoad >= maxLoad";
    if (f.grow <= 0) return "Grow <= 0.0";
    if (f.shrink <= 0) return "Shrink <= 0.0";
    //
    // The shrink factor requires additional validation because we want to make sure that
    // rehash down will always succeed, i.e., you can't shrink TOO much or you're toast.
    // (because it won't fit ;-))
    //
    if (f.shrink > (1.0f - f.minLoad)) return "Shrink too large";
    return nullptr;  // We're good !!!
}

/******************************************************************************************
 * Implement KeyTable_Layout
 *
 * Efficiently store three quantities in sequential memory: RefCount, Length, Text[0..Length-1]
 *
 * We use either 1, 2, 3 or 4 bytes to store the length.
 */

// Maximum legal refcount. 2^29-1
static uint32_t MAX_REF_COUNT = 0x1FFFFFFF;

bool KeyTable_Layout::IsStuck() const {
    return refCount >= MAX_REF_COUNT;
}

bool KeyTable_Layout::incrRefCount() const {
    if (IsStuck()) {
        return true;  // Saturated
    } else {
        refCount++;
        return false;
    }
}

size_t KeyTable_Layout::decrRefCount() const {
    KEYTABLE_ASSERT(refCount > 0);
    if (!IsStuck()) refCount--;
    return refCount;
}

size_t KeyTable_Layout::getLength() const {
    // Length is stored in little-endian format
    size_t len = 0;
    for (size_t i = 0; i <= lengthBytes; ++i) {
        len |= *reinterpret_cast<const uint8_t *>(bytes+i) << (i * 8);
    }
    return len;
}

const char *KeyTable_Layout::getText() const {
    return bytes + lengthBytes + 1;
}

KeyTable_Layout *KeyTable_Layout::makeLayout(void *(*malloc)(size_t), const char *ptr, size_t len,
                                             size_t hash, bool noescape) {
    size_t lengthBytes = (len <= 0xFF) ? 1 :
                         (len <= 0xFFFF) ? 2:
                         (len <= 0xFFFFFF) ? 3:
                         (len <= 0xFFFFFFFF) ? 4 :
                         0;
    KeyTable_Layout *p = reinterpret_cast<KeyTable_Layout*>(malloc(
            sizeof(KeyTable_Layout) + lengthBytes + len));
    p->original_hash = hash;
    p->noescapeFlag = noescape;
    p->refCount = 1;
    p->lengthBytes = lengthBytes - 1;
    // store the length in little-endian format
    for (size_t i = 0; i < lengthBytes; ++i) {
        p->bytes[i] = len >> (8 * i);
    }
    std::memcpy(p->bytes + lengthBytes, ptr, len);
    return p;
}


// Unit test only.
void KeyTable_Layout::setMaxRefCount(uint32_t maxRefCount) {
    KEYTABLE_ASSERT(sizeof(KeyTable_Layout) == 5 + 8);
    KEYTABLE_ASSERT(maxRefCount <= MAX_REF_COUNT);           // can only shrink it.
    MAX_REF_COUNT = maxRefCount;
}

