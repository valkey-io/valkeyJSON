#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <deque>
#include <map>
#include <string>
#include <sstream>
#include <random>
#include <limits>
#include <vector>
#include <cmath>
#include <utility>
#include <gtest/gtest.h>
#include "json/dom.h"
#include "json/alloc.h"
#include "json/stats.h"
#include "json/keytable.h"
#include "module_sim.h"

// Cheap, predictable hash
static size_t hash1(const char *ptr, size_t len) {
    (void)ptr;
    return len;
}

class HashTableTest : public ::testing::Test {
 protected:
    void SetUp() override {
    }
    size_t original_malloced;
    void TearDown() override {
        if (keyTable) {
            malloced = original_malloced;
            EXPECT_EQ(keyTable->validate(), "");
        }
        delete keyTable;
    }
    void Setup1(size_t numShards = 1, size_t htsize = 0, size_t (*h)(const char *, size_t) = hash1) {
        setupValkeyModulePointers();
        KeyTable::Config c;
        c.malloc = dom_alloc;
        c.free = dom_free;
        c.hash = h;
        c.numShards = numShards;
        keyTable = new KeyTable(c);
        rapidjson::hashTableFactors.minHTSize = htsize;
        original_malloced = malloced;
        malloced = 0;  // Ignore startup memory consumption
    }
};

TEST_F(HashTableTest, simple) {
    Setup1();
    {
        JValue v;
        v.SetObject();
        v.AddMember(JValue("True"), JValue(true), allocator);
        EXPECT_EQ(v.MemberCount(), 1u);
        EXPECT_TRUE(v["True"].IsBool());
        EXPECT_GT(malloced, 0);
    }
    EXPECT_EQ(malloced, 0);
}

static JValue makeKey(size_t i) {
    return std::move(JValue().SetString(std::to_string(i), allocator));
}

static JValue makeArray(size_t sz, size_t offset = 0) {
    JValue j;
    j.SetArray();
    for (size_t i = 0; i < sz; ++i) {
        j.PushBack(JValue(i + offset), allocator);
    }
    return j;
}

static JValue makeArrayArray(size_t p, size_t q) {
    JValue j = makeArray(p);
    for (size_t i = 0; i < p; ++i) {
        j[i] = makeArray(q, i);
    }
    return j;
}

TEST_F(HashTableTest, checkeq) {
    Setup1();
    for (size_t i : {0, 1, 10}) {
        ASSERT_EQ(makeArrayArray(i, i), makeArrayArray(i, i));
    }
}

TEST_F(HashTableTest, insertAndRemoveMany) {
    Setup1(1, 5);
    for (size_t sz : {10, 50, 100}) {
        EXPECT_EQ(malloced, 0);
        rapidjson::hashTableStats.reset();
        {
            JValue v;
            v.SetObject();
            EXPECT_EQ(v.Validate(), "");
            for (size_t i = 0; i < sz; ++i) {
                v.AddMember(makeKey(i), makeArrayArray(i, i), allocator);
                EXPECT_EQ(v.Validate(), "");
            }
            EXPECT_EQ(v.MemberCount(), sz);
            EXPECT_GT(rapidjson::hashTableStats.rehashUp, 0);
            EXPECT_EQ(rapidjson::hashTableStats.convertToHT, 1);
            auto s = keyTable->getStats();
            EXPECT_EQ(s.size, sz);
            for (size_t i = 0; i < sz; ++i) EXPECT_EQ(v[makeKey(i)], makeArrayArray(i, i));
            for (size_t i = 0; i < sz; ++i) {
                v.RemoveMember(makeKey(i));
                EXPECT_EQ(v.Validate(), "");
            }
            EXPECT_GT(rapidjson::hashTableStats.rehashDown, 0);
            EXPECT_EQ(v.MemberCount(), 0);
            s = keyTable->getStats();
            EXPECT_EQ(s.size, 0);  // All entries should be gone.
        }
        EXPECT_EQ(malloced, 0);
    }
}

TEST_F(HashTableTest, SetObjectRawHT) {
    Setup1();
    std::ostringstream os;
    os << "{\"a\":1";
    for (size_t i = 0; i < 100; ++i) os << ",\"" << i << "\":" << i;
    os << "}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, os.str().c_str(), os.str().size(), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(rapidjson::hashTableStats.reserveHT, 1);
    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(oss.GetString(), os.str());

    dom_free_doc(doc);
    auto s = keyTable->getStats();
    EXPECT_EQ(s.size, 0);  // All entries should be gone.
    EXPECT_EQ(malloced, 0);
}

TEST_F(HashTableTest, CopyMembers) {
    Setup1(1, 5);
    for (size_t sz : {10, 50, 100}) {
        rapidjson::hashTableStats.reset();
        JValue v;
        v.SetObject();
        EXPECT_EQ(v.Validate(), "");
        for (size_t i = 0; i < sz; ++i) {
            v.AddMember(makeKey(i), makeArrayArray(i, i), allocator);
            EXPECT_EQ(v.Validate(), "");
        }
        EXPECT_EQ(v.MemberCount(), sz);
        EXPECT_GT(rapidjson::hashTableStats.rehashUp, 0);
        EXPECT_EQ(rapidjson::hashTableStats.convertToHT, 1);
        auto s = keyTable->getStats();
        EXPECT_EQ(s.size, sz);
        EXPECT_EQ(s.handles, sz);
        {
            rapidjson::hashTableStats.reset();
            JValue v2(v, allocator);  // Invokes copymembers
            EXPECT_EQ(v2.Validate(), "");
            EXPECT_EQ(v2.MemberCount(), sz);
            EXPECT_EQ(rapidjson::hashTableStats.rehashUp, 0);
            EXPECT_EQ(rapidjson::hashTableStats.rehashDown, 0);
            EXPECT_EQ(rapidjson::hashTableStats.convertToHT, 0);
            s = keyTable->getStats();
            EXPECT_EQ(s.size, sz);
            EXPECT_EQ(s.handles, sz*2);
            for (size_t i = 0; i < sz; ++i) {
                EXPECT_EQ(v[makeKey(i)].GetArray(), makeArrayArray(i, i));
                EXPECT_EQ(v2[makeKey(i)].GetArray(), makeArrayArray(i, i));
            }
        }
    }
    EXPECT_EQ(malloced, 0);
}

//
// Test that hash tables > 2^19 are properly handled.
//
TEST_F(HashTableTest, DistributionTest) {
    extern size_t hash_function(const char *, size_t);
    Setup1(1, 0, hash_function);
    enum { TABLE_SIZE_BITS = 22 };  // LOG2(Table Size)
    enum { TABLE_SIZE = 1ull << TABLE_SIZE_BITS };
    JValue v;
    v.SetObject();
    for (size_t i = 0; i < TABLE_SIZE; ++i) {
        v.AddMember(makeKey(i), JValue(true), allocator);
    }
    //
    // Now, compute the distribution stats, make sure the longest run is sufficiently small
    //
    std::map<size_t, size_t> runs;
    v.getObjectDistribution(runs, 5);
    //  std::cout << "Dist:";
    //  for (auto& x : runs) std::cout << x.first << ":" << x.second << ",";
    //  std::cout << std::endl;
    ASSERT_NE(runs.size(), 0u);
    EXPECT_LT(runs.rbegin()->first, 0.0001 * TABLE_SIZE);
}
