#ifndef _KEYTABLE_H
#define _KEYTABLE_H

/************************************************************************************************
 *
 * The key table. This thread-safe object implements unique use-counted immutable strings.
 *
 * This object is a repository of immutable strings. You put a string into the repository
 * and you get back an immutable handle (8-bytes). The handle can be cheaply de-referenced to yield the
 * underlying string text (when needed). When you're done with the handle you give it back to the
 * string table. So far, nothing special.
 *
 * The key table maintains a reference count for each string in the table AND it guarantees that
 * each string in the table is unique. Further, two insertions of the same string will yield the
 * same handle, meaning that once you've converted a string into a handle you can do equality
 * comparisons on other strings simply by comparing the handles for equality.
 *
 * After initialization, there are only two operations on the global hashtable.
 *
 * (1) Insert string, return handle. (string is copied, the caller can discard his memory)
 * (2) discard handle.
 *
 * Both operations are thread-safe and the handles are NOT locked to a thread.
 * The handle represents a resource allocation within the table and thus every call to (1) must
 * eventually have a call to (2) to release the resource (i.e., decrement the refcount)
 *
 * **********************************************************************************************
 */

/*
 * IMPLEMENTATION
 *
 * Each unique string (with it's metadata, length & refcount) is stored in a separately malloc'ed
 * chunk of memory. The handle contains a pointer to this data. Thus having obtained a handle,
 * access to the underlying string is trivially cheap. Since the handle and the string itself are
 * immutable, no thread locking need be done to access the data.
 *
 * A separate data structure (table & shards) contains a mapping table that implements the raw
 * API to convert a string to a handle. That mapping table is sharded with each shard being
 * protected by a mutex. A string is received and hashed. The hashcode selects the shard for that
 * string. The shard is locked, the mapping table is consulted to locate a previous copy of the
 * string. If the string is found, the refcount is incremented and a handle is constructed from
 * the existing pointer and shard number. If the string isn't found, a new malloc string is created,
 * the mapping is updated and a handle is constructed and returned.
 *
 * The mapping is implemented as a hashtable using linear hashing. Each hash table entry is simply
 * the malloc'ed pointer and 19-bits of hash code. Various conditions can cause a rehashing event,
 * rehashing is always done as a single operation on the entire hashtable while the mutex is held,
 * i.e., there is no incremental re-hashing. This makes it very easy to ensure multi-thread correctness.
 * Worst-case CPU loads due to rehashing are limited because the size of a shard hashtable is itself
 * limited to 2^19 entries. You vary the number of shards to handle the worst-case number of strings
 * in the table.
 *
 * The refcount for a string is currently fixed at 30-bits. Increment and decrement of the refcount
 * is done with saturating arithmetic, meaning that if a string ever hits the maximum refcount it
 * will never be deleted from the table. This isn't considered to be a problem.
 *
 */

#include <atomic>
#include <string>
#include <ostream>
#include <string_view>
#include <map>
#include <unordered_map>
#include "json/alloc.h"

#ifndef KEYTABLE_ASSERT
#define KEYTABLE_ASSERT(x) RAPIDJSON_ASSERT(x)
#endif

/*
 * This class implements a pointer with up to 19 bits of additional metadata. On x86_64 and Aarch-64
 * There are 16 bits at the top of the pointer that are unused (guaranteed to be zero) and we assume
 * that the pointer references malloc'ed memory with a minimum of 8-byte alignment, guaranteeing that
 * another 3 bits of usable storage.
 *
 * Other versions of this class could be implemented for systems that don't meet the requirements
 * above and simply store the metadata adjacent to a full pointer (i.e., 32-bit systems).
 *
 */
template<typename T>
class PtrWithMetaData {
 public:
    enum { METADATA_MASK = (1 << 19)-1 };           // Largest value that fits.

    const T& operator*() const { return *getPointer(); }
    const T* operator->() const { return getPointer(); }
    T& operator*() { return *getPointer(); }
    T* operator->() { return getPointer(); }

    //
    // It's "C"-ism, that you test pointers for null/!null by doing "if (ptr)" or "if (!ptr)"
    // C++ considers that as a conversion to a boolean. Which is implemented by this operator
    // To be clear, we include the Metadata in the comparison.
    //
    operator bool() const { return bits != 0; }   // if (PtrWithMetaData) invokes this operator

    size_t getMetaData() const { return ror(bits, 48) & METADATA_MASK; }
    void setMetaData(size_t metadata) {
        KEYTABLE_ASSERT(0 == (metadata & ~METADATA_MASK));
        bits = (bits & PTR_MASK) | ror(metadata, 16);
    }
    PtrWithMetaData() : bits(0) {}
    PtrWithMetaData(T *ptr, size_t metadata) {
        KEYTABLE_ASSERT(0 == (~PTR_MASK & reinterpret_cast<size_t>(ptr)));
        KEYTABLE_ASSERT(0 == (metadata & ~METADATA_MASK));
        bits = reinterpret_cast<size_t>(ptr) | ror(metadata, 16);
    }
    void clear() { bits = 0; }
    //
    // Comparison operations also include the metadata
    //
    bool operator==(const PtrWithMetaData& rhs) const { return bits == rhs.bits; }
    bool operator!=(const PtrWithMetaData& rhs) const { return bits != rhs.bits; }

    friend std::ostream& operator<<(std::ostream& os, const PtrWithMetaData& ptr) {
        return os << "Ptr:" << reinterpret_cast<const void *>(&*ptr) << " MetaData:" << ptr.getMetaData();
    }

    void swap(PtrWithMetaData& rhs) { std::swap<size_t>(bits, rhs.bits); }

 private:
    size_t bits;
    T* getPointer() const { return MEMORY_VALIDATE<T>(reinterpret_cast<T *>(bits & PTR_MASK)); }
    // Circular rotate right (count <= 64)
    static constexpr size_t ror(size_t v, unsigned count) {
        return (v >> count) | (v << (64-count));
    }
    static const size_t PTR_MASK = ~ror(METADATA_MASK, 16);
};

//
// This is the struct that's accessed by dereferencing a KeyTable_Handle.
// Normal users should only look at len and text fields -- and consider them immutable.
// For Normal users, the only statement about refcount is that it will be non-zero as long as
// any handle exists.
//
// Privileged unit tests look at the refcount field also...
//
struct KeyTable_Layout {
    //
    // Create a string layout. allocates some memory
    //
    static KeyTable_Layout *makeLayout(void *(*malloc)(size_t), const char *ptr,
                                       size_t len, size_t hash, bool noescape);
    //
    // Interrogate existing layout
    //
    size_t getRefCount() const { return refCount; }
    size_t getLength() const;
    const char *getText() const;
    bool IsStuck() const;
    bool getNoescape() const { return noescapeFlag != 0; }
    enum { POISON_VALUE = 0xdeadbeeffeedfeadull };
    size_t getOriginalHash() const { return original_hash; }
    void poisonOriginalHash() { original_hash = POISON_VALUE; }
    bool isPoisoned() const { return original_hash == POISON_VALUE; }
    // Unit test
    static void setMaxRefCount(uint32_t maxRefCount);

 protected:
    KeyTable_Layout();               // Nobody gets to create one.
    friend class KeyTable_Shard;     // Only class allowed to manipulate reference count
    bool incrRefCount() const;       // true => saturated
    size_t decrRefCount() const;     // returns current count
    size_t original_hash;            // Remember original hash
    mutable uint32_t refCount:29;    // Ref count.
    uint32_t noescapeFlag:1;         // String doesn't need to be escaped
    uint32_t lengthBytes:2;          // 0, 1, 2 or 3 => 1, 2, 3 or 4 bytes of length
    char bytes[1];                   // length bytes + text bytes
} __attribute__((packed));           // Don't let compiler round size of up 8 bytes.

struct KeyTable_Handle {
    /***************************** Public Handle Interface *******************************/
    //
    // get a pointer to the text of the string. This pointer has the same lifetime as the
    // string_table_handle object itself.
    //
    const KeyTable_Layout& operator*() const { return *theHandle; }
    const KeyTable_Layout* operator->() const { return &*theHandle; }
    const char *GetString() const { return theHandle->getText(); }
    size_t GetStringLength() const { return theHandle->getLength(); }
    const std::string_view GetStringView() const
        { return std::string_view(theHandle->getText(), theHandle->getLength()); }
    size_t GetHashcode() const { return theHandle.getMetaData(); }
    bool IsNoescape() const { return theHandle->getNoescape(); }

    enum { MAX_HASHCODE = PtrWithMetaData<KeyTable_Layout>::METADATA_MASK };
    //
    // Assignment is only allowed into a empty handle.
    //
    KeyTable_Handle& operator=(const KeyTable_Handle& rhs) {
        KEYTABLE_ASSERT(!theHandle);
        theHandle = rhs.theHandle;
        const_cast<KeyTable_Handle&>(rhs).theHandle.clear();
        return *this;
    }
    //
    // Do assignment into raw storage
    //
    void RawAssign(const KeyTable_Handle& rhs) {
        theHandle = rhs.theHandle;
        const_cast<KeyTable_Handle&>(rhs).theHandle.clear();
    }
    //
    // move semantics are allowed
    //
    KeyTable_Handle(KeyTable_Handle&& rhs) {
        theHandle = rhs.theHandle;
        rhs.theHandle.clear();
    }
    //
    // Comparison
    //
    bool operator==(const KeyTable_Handle& rhs) const { return theHandle == rhs.theHandle; }
    bool operator!=(const KeyTable_Handle& rhs) const { return theHandle != rhs.theHandle; }

    operator bool() const { return bool(theHandle); }

    friend std::ostream& operator<<(std::ostream& os, const KeyTable_Handle& h) {
        return os << "Handle:" << reinterpret_cast<const void *>(&*(h.theHandle))
            << " Shard:" << h.theHandle.getMetaData()
            << " RefCount: " << h->getRefCount()
            << " : " << h.GetStringView();
    }

    KeyTable_Handle() : theHandle() {}
    ~KeyTable_Handle() { KEYTABLE_ASSERT(!theHandle); }

    void Swap(KeyTable_Handle& rhs) {
        theHandle.swap(rhs.theHandle);
    }

 private:
    friend class KeyTable;
    friend struct KeyTable_Shard;

    KeyTable_Handle(KeyTable_Layout *ptr, size_t hashCode) : theHandle(ptr, hashCode) {}
    void clear() { theHandle.clear(); }

    PtrWithMetaData<KeyTable_Layout> theHandle;                   // The only actual data here.
};

/*
 * This is the core hashtable, it's invisible externally
 */
struct KeyTable_Shard;

struct KeyTable {
    /*************************** External Table Interface *********************************/

    enum { MAX_SHARDS = KeyTable_Handle::MAX_HASHCODE, MIN_SHARDS = 1 };


    //
    // Stuff to create a table. These get copied and can't be changed without
    // recreating the entire table.
    //
    struct Config {
        void *(*malloc)(size_t);                            // Use this to allocate memory
        void (*free)(void*);                                // Use this to free memory
        size_t (*hash)(const char *, size_t);               // Hash function for strings
        size_t numShards;                                   // Number of shards to create
    };
    //
    // Construct a table.
    //
    explicit KeyTable(const Config& cfg);
    ~KeyTable();
    //
    // Make a handle for this string. The target string is copied when necessary.
    //
    KeyTable_Handle makeHandle(const char *ptr, size_t len, bool noescape = false);
    KeyTable_Handle makeHandle(const std::string& s, bool noescape = false) {
        return makeHandle(s.c_str(), s.length(), noescape);
    }
    KeyTable_Handle makeHandle(const std::string_view& s, bool noescape = false) {
        return makeHandle(s.data(), s.length(), noescape);
    }

    KeyTable_Handle clone(const KeyTable_Handle& rhs);
    //
    // Destroy a handle
    //
    void destroyHandle(KeyTable_Handle &h);
    //
    // Some of the configuration variables can be changed dynamically.
    //
    struct Factors {
        float minLoad;          // LoadFactor() < minLoad => rehash down
        float maxLoad;          // LoadFactor() > maxLoad => rehash up
        float shrink;           // % to shrink by
        float grow;             // % to grow by
        Factors() :
            // Default Factors for the hash table
            minLoad(0.25f),                          // minLoad => .25
            maxLoad(0.85f),                          // maxLoad => targets O(8) searches [see wikipedia]
            shrink(0.5f),                            // shrink, remove 1/2 of elements.
            grow(1.0f)                               // Grow by 100%
            {}
    };

    //
    // Get the current configuration
    //
    const Factors& getFactors() const { return factors; }
    //
    // Query if this set of factors is valid.
    // returns: NULL, If the factors are valid. Otherwise an error string
    // This is used to validate a set of factors before setting them.
    //
    static const char *isValidFactors(const Factors& f);
    //
    // Change to these factors if valid. This is modestly expensive as it grabs all shard locks
    // This will assert if the factors are invalid.
    //
    void setFactors(const Factors& proposed);

    /*
     * Stats you can get at any time.
     *
     * Reading these is O(numShards) which can be expensive
     *
     * These stats are computed by summing up across the shards. Each shard is locked and
     * then it's contribution is added to the running totals. Because of the time skew for
     * the reading, there maybe slight inaccuracies in the presence of multi-thread operations.
     */
    struct Stats {
        size_t size;                // Total number of unique strings in table
        size_t bytes;               // Total bytes of strings
        size_t handles;             // Number of outstanding handles
        size_t maxTableSize;        // Largest Shard table
        size_t minTableSize;        // Smallest Shard table
        size_t totalTable;          // sum of table sizes
        size_t stuckKeys;        // Number of strings that have hit the refcount max.
        //
        // These counters are reset after being read.
        //
        size_t maxSearch;           // longest search sequence encountered
        size_t rehashes;            // Number of rehashes
        //
        // Include a copy of current settable factors. Makes testing easier
        //
        Factors factors;
    };
    Stats getStats() const;

    //
    // Long stats are stats that are VERY expensive to compute and are generally only
    // used for debug or unit tests. You can see these in the JSON.DEBUG command provided
    // you are coming in from an Admin connection.
    //
    struct LongStats {
        std::map<size_t, size_t> runs;  // size of of runs, count of #runs
    };
    //
    // TopN parameter limits size of result to largest N runs.
    //   Setting N to a relatively small number will reduce the cost of generating the stats.
    //
    LongStats getLongStats(size_t topN) const;

    KeyTable(const KeyTable& rhs) = delete;  // no copies
    void operator=(const KeyTable& rhs) = delete;  // no assignment

    std::string validate() const;  // Unit testing only
    std::string validate_counts(std::unordered_map<const KeyTable_Layout *, size_t>& counts) const;  // Debug command

    size_t getNumShards() const { return numShards; }

 private:
    friend class KeyTable_Shard;
    size_t shardNumberFromHash(size_t hash);
    size_t hashcodeFromHash(size_t hash);
    KeyTable_Shard* shards;
    void *(*malloc)(size_t);                // Use this to allocate memory
    void (*free)(void *);                   // Use this to free memory
    size_t (*hash)(const char *, size_t);   // Hash function for strings
    size_t numShards;
    std::atomic<size_t> stuckKeys;       // Stuck String count.
    Factors factors;
};

extern KeyTable *keyTable;      // The singleton

#endif
