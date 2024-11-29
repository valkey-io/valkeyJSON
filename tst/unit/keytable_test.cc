#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <deque>
#include <string>
#include <sstream>
#include <random>
#include <limits>
#include <vector>
#include <cmath>
#include <gtest/gtest.h>
#include "json/dom.h"
#include "json/alloc.h"
#include "json/stats.h"
#include "json/keytable.h"
#include "json/memory.h"
#include "module_sim.h"

class PtrWithMetaDataTest : public ::testing::Test {
};

TEST_F(PtrWithMetaDataTest, t) {
    memory_traps_control(false);   // Necessary so that MEMORY_VALIDATE buried in getPointer doesn't croak on bad memory
    EXPECT_EQ(0x7FFFF, PtrWithMetaData<size_t>::METADATA_MASK);
    for (size_t i = 1; i & 0x7FFFF; i <<= 1) {
        size_t var = 0xdeadbeeffeedfeddull;
        PtrWithMetaData<size_t> p(&var, i);
        EXPECT_EQ(&*p, &var);
        EXPECT_EQ(*p, var);
        EXPECT_EQ(size_t(p.getMetaData()), i);
        p.clear();
        EXPECT_EQ(p.getMetaData(), 0);
        p.setMetaData(i);
        EXPECT_EQ(size_t(p.getMetaData()), i);
    }
    for (size_t i = 8; i & 0x0000FFFFFFFFFFF8ull; i <<= 1) {
        PtrWithMetaData<size_t> p(reinterpret_cast<size_t *>(i), 0x7FFFF);
        EXPECT_EQ(size_t(&*p), i);
        EXPECT_EQ(p.getMetaData(), 0x7FFFF);
    }
}

// Cheap, predictable hash
static size_t hash1(const char *ptr, size_t len) {
    (void)ptr;
    return len;
}

extern size_t MAX_FAST_TABLE_SIZE;      // in keytable.cc

class KeyTableTest : public ::testing::Test {
 protected:
    void SetUp() override {
    }

    void TearDown() override {
        if (t) {
            EXPECT_EQ(t->validate(), "");
        }
        delete t;
    }

    void Setup1(size_t numShards = 1, size_t (*hf)(const char *, size_t) = hash1) {
        setupValkeyModulePointers();
        KeyTable::Config c;
        c.malloc = dom_alloc;
        c.free = dom_free;
        c.hash = hf;
        c.numShards = numShards;
        t = new KeyTable(c);
    }

    KeyTable *t = nullptr;
};

TEST_F(KeyTableTest, layoutTest) {
    Setup1();

    size_t bias = 10;
    for (size_t slen : {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                        0xFF, 0x100, 0xFFFF, 0x10000, 0xFFFFFF, 0x1000000}) {
        std::string s;
        s.resize(slen);
        for (size_t i = 0; i < slen; ++i) {
            s[i] = i + bias;
        }
        KeyTable_Layout *l = KeyTable_Layout::makeLayout(dom_alloc, s.data(), s.length(), 0, false);
        ASSERT_EQ(l->getLength(), slen);
        for (size_t i = 0; i < slen; ++i) {
            ASSERT_EQ(0xFF & (i + bias), 0xFF & (l->getText()[i]));
        }
        dom_free(l);
        bias++;
    }
}

TEST_F(KeyTableTest, testInitialization) {
    Setup1();
    EXPECT_EQ(t->validate(), "");
    EXPECT_GT(malloced, 0);
    EXPECT_EQ(t->validate(), "");
    auto s = t->getStats();
    EXPECT_EQ(s.size, 0);
    EXPECT_EQ(s.handles, 0);
    EXPECT_EQ(s.bytes, 0);
    EXPECT_GT(s.maxTableSize, 0);
    EXPECT_GT(s.totalTable, 0);
    EXPECT_EQ(s.rehashes, 0);
    EXPECT_EQ(s.maxSearch, 0);
    delete t;
    t = nullptr;
    EXPECT_EQ(malloced, 0);
}

TEST_F(KeyTableTest, testDuplication) {
    std::string e = "Empty";
    Setup1();
    auto f = t->getFactors();
    f.maxLoad = 1.0;  // No rehashes until we're full....
    f.minLoad = std::numeric_limits<float>::min();
    t->setFactors(f);
    KeyTable_Handle h1 = t->makeHandle(e);
    EXPECT_TRUE(h1);
    EXPECT_EQ(t->validate(), "");
    KeyTable_Handle h2 = t->makeHandle(e);
    EXPECT_EQ(t->validate(), "");
    EXPECT_TRUE(h2);
    EXPECT_EQ(h1, h2);
    EXPECT_EQ(&*h1, &*h2);
    auto s = t->getStats();
    EXPECT_EQ(s.size, 1);
    EXPECT_EQ(s.handles, 2);
    EXPECT_EQ(s.bytes, 5);
    t->destroyHandle(h1);
    EXPECT_TRUE(!h1);
    EXPECT_EQ(t->validate(), "");
    s = t->getStats();
    EXPECT_EQ(s.size, 1);
    EXPECT_EQ(s.handles, 1);
    EXPECT_EQ(s.bytes, 5);
    t->destroyHandle(h2);
    EXPECT_TRUE(!h2);
    EXPECT_EQ(t->validate(), "");
    s = t->getStats();
    EXPECT_EQ(s.rehashes, 0);
}

TEST_F(KeyTableTest, testClone) {
    std::string e = "Empty";
    Setup1();
    auto f = t->getFactors();
    f.maxLoad = 1.0;  // No rehashes until we're full....
    f.minLoad = std::numeric_limits<float>::min();
    t->setFactors(f);
    KeyTable_Handle h1 = t->makeHandle(e);
    EXPECT_TRUE(h1);
    EXPECT_EQ(t->validate(), "");
    KeyTable_Handle h2 = t->clone(h1);
    EXPECT_EQ(t->validate(), "");
    EXPECT_TRUE(h2);
    EXPECT_EQ(h1, h2);
    EXPECT_EQ(&*h1, &*h2);
    auto s = t->getStats();
    EXPECT_EQ(s.size, 1);
    EXPECT_EQ(s.handles, 2);
    EXPECT_EQ(s.bytes, 5);
    t->destroyHandle(h1);
    EXPECT_TRUE(!h1);
    EXPECT_EQ(t->validate(), "");
    s = t->getStats();
    EXPECT_EQ(s.size, 1);
    EXPECT_EQ(s.handles, 1);
    EXPECT_EQ(s.bytes, 5);
    t->destroyHandle(h2);
    EXPECT_TRUE(!h2);
    EXPECT_EQ(t->validate(), "");
    s = t->getStats();
    EXPECT_EQ(s.rehashes, 0);
}

TEST_F(KeyTableTest, SimpleRehash) {
    Setup1(1);  // 4 element table is the minimum.
    auto f = t->getFactors();
    f.maxLoad = 1.0;  // No rehashes until we're full....
    f.minLoad = std::numeric_limits<float>::min();
    f.grow = 1.0;
    f.shrink = 0.5;
    t->setFactors(f);
    std::vector<KeyTable_Handle> h;
    std::vector<std::string> keys;
    std::string k = "";
    for (size_t i = 0; i < 4; ++i) {
        h.push_back(t->makeHandle(k));
        keys.push_back(k);
        auto s = t->getStats();
        EXPECT_EQ(s.size, i+1);
        EXPECT_EQ(s.rehashes, 0);
        EXPECT_EQ(t->validate(), "");
        k += '*';
    }
    for (size_t i = 4; i < 8; ++i) {
        auto f = t->getFactors();
        f.maxLoad = i == 4 ? .5 : 1.0;  // No rehashes until we're full....
        t->setFactors(f);

        h.push_back(t->makeHandle(k));
        keys.push_back(k);
        auto s = t->getStats();
        EXPECT_EQ(s.size, i+1);
        EXPECT_EQ(s.rehashes, i == 4 ? 1 : 0);
        EXPECT_EQ(t->validate(), "");
        k += '*';
    }
    //
    // Now shrink
    //
    for (size_t i = 0; i < 4; ++i) {
        t->destroyHandle(h.back());
        h.pop_back();
        auto s = t->getStats();
        EXPECT_EQ(s.rehashes, 0);
        EXPECT_EQ(t->validate(), "");
    }
    // Next destroyHandle should case a rehash.
    for (size_t i = 0; i < 4; ++i) {
        auto f = t->getFactors();
        f.minLoad = i == 0 ? .5f : std::numeric_limits<float>::min();  // No rehashes until we're full....
        t->setFactors(f);
        t->destroyHandle(h.back());
        h.pop_back();
        auto s = t->getStats();
        EXPECT_EQ(s.maxTableSize, 4);
        EXPECT_EQ(s.rehashes, i == 0 ? 1 : 0);
        EXPECT_EQ(t->validate(), "");
    }
}

//
// Generate some strings, duplicates are ok.
// Because the hash is the length + the last character the total number of unique strings
// is only 10x of the max length (from the random distribution)
//
std::default_random_engine generator(0);
std::uniform_int_distribution dice(0, 10000);       // there are actually ~10x this number of unique strings
size_t make_rand() {
    return dice(generator);
}

std::string make_key() {
    size_t len = make_rand();
    size_t lastDigit = make_rand() % 10;
    std::string k;
    for (size_t i = 0; i < len; ++i) k += '*';
    k += '0' + lastDigit;
    return k;
}

TEST_F(KeyTableTest, BigTest) {
    //
    // Make a zillion keys, Yes, there will be lots of duplicates -> Intentionally
    //
    for (size_t ft : { 1 << 8, 1 << 10, 1 << 12}) {
        MAX_FAST_TABLE_SIZE = ft;
        for (size_t numShards : {1, 2}) {
            for (size_t numKeys : {1000}) {
                Setup1(numShards);
                auto f = t->getFactors();
                f.grow = 1.1;           // Grow slowly
                f.maxLoad = .95;        // Let the table get REALLY full between hashes
                t->setFactors(f);
                std::vector<KeyTable_Handle> h;
                std::vector<std::string> k;
                for (size_t i = 0; i < numKeys; ++i) {
                    k.push_back(make_key());
                    h.push_back(t->makeHandle(k.back().c_str(), k.back().length()));
                    if (0 == (i & 0xFF)) {
                        EXPECT_EQ(t->validate(), "");
                    }
                }
                auto s = t->getStats();
                EXPECT_EQ(s.handles, k.size());
                EXPECT_LT(s.size, k.size());            // must have at least one duplicate
                EXPECT_GT(s.rehashes, 5);              // should have had several rehashes
                //
                // now delete them SLOWLY with lots of rehashes
                //
                f = t->getFactors();
                f.shrink = .05;  // Shrink slowly
                f.minLoad = .9;        // Let the table get REALLY full between hashes
                t->setFactors(f);
                for (size_t i = 0; i < numKeys; ++i) {
                    t->destroyHandle(h[i]);
                    if (0 == (i & 0xFF)) {
                        EXPECT_EQ(t->validate(), "");
                    }
                }
                //
                // Teardown.
                //
                EXPECT_EQ(t->validate(), "");
                s = t->getStats();
                EXPECT_GT(s.rehashes, 10);
                EXPECT_EQ(s.size, 0);
                delete t;
                t = nullptr;
            }
        }
    }
}

TEST_F(KeyTableTest, StuckKeys) {
    Setup1(1);
    KeyTable_Layout::setMaxRefCount(3);
    std::string e = "Empty";
    KeyTable_Handle h1 = t->makeHandle(e);
    KeyTable_Handle h2 = t->makeHandle(e);
    KeyTable_Handle h3 = t->makeHandle(e);
    KeyTable_Handle h4 = t->makeHandle(e);
    EXPECT_EQ(t->validate(), "");
    auto s = t->getStats();
    EXPECT_EQ(s.size, 1);
    EXPECT_EQ(s.stuckKeys, 1);
    EXPECT_EQ(s.handles, 4);
    t->destroyHandle(h1);
    t->destroyHandle(h2);
    t->destroyHandle(h3);
    t->destroyHandle(h4);
    s = t->getStats();
    EXPECT_EQ(s.stuckKeys, 1);
    EXPECT_EQ(s.size, 1);
    EXPECT_EQ(s.handles, 0);
}

//
// Make a very large shard, check some stats, delete the elements and see if it shrinks
//
extern size_t hash_function(const char *, size_t);

TEST_F(KeyTableTest, BigShard) {
    memory_traps_control(false);
    Setup1(1, hash_function);
    enum { TABLE_SIZE_BITS = 22 };  // LOG2(Table Size)
    enum { TABLE_SIZE = 1ull << TABLE_SIZE_BITS };
    std::vector<KeyTable_Handle> handles1;
    std::vector<KeyTable_Handle> handles2;
    //
    // Fill up the table
    //
    for (size_t i = 0; i < TABLE_SIZE; ++i) {
        handles1.push_back(t->makeHandle(std::to_string(i)));
    }
    auto s = t->getStats();
    EXPECT_EQ(s.size, TABLE_SIZE);
    EXPECT_EQ(s.handles, TABLE_SIZE);
    EXPECT_LE(s.rehashes, TABLE_SIZE_BITS);
    //
    // Check hash table distribution
    //
    auto ls = t->getLongStats(2);
    EXPECT_EQ(ls.runs.size(), 2);
    EXPECT_LT(ls.runs.rbegin()->first, 100);  // Only look at second longest run
    //
    // Duplicate add of Handle
    //
    for (size_t i = 0; i < TABLE_SIZE; ++i) {
        handles2.push_back(t->makeHandle(std::to_string(i)));
        EXPECT_EQ(handles1[i], handles2[i]);
    }
    s = t->getStats();
    EXPECT_EQ(s.size, TABLE_SIZE);
    EXPECT_LE(s.rehashes, 0);
    EXPECT_EQ(s.handles, 2*TABLE_SIZE);
    //
    // Now, delete each handle once. Basically nothing about the table should change
    //
    for (auto& h : handles1) { t->destroyHandle(h); }
    s = t->getStats();
    EXPECT_EQ(s.size, TABLE_SIZE);
    EXPECT_EQ(s.handles, TABLE_SIZE);
    EXPECT_EQ(s.maxSearch, 0);
    EXPECT_EQ(s.rehashes, 0);
    //
    // Now empty the table
    //
    for (auto& h : handles2) { t->destroyHandle(h); }
    s = t->getStats();
    EXPECT_EQ(s.size, 0);
    EXPECT_EQ(s.handles, 0);
    EXPECT_GT(s.rehashes, TABLE_SIZE_BITS - 3);  // Minimum table size
}
