#undef NDEBUG
#include <assert.h>
#include <stdarg.h>
#include <signal.h>

#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <cmath>
#include <memory>
#include <deque>
#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include <iostream>
#include <unordered_map>
#include <map>
#include <set>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "json/alloc.h"
#include "json/dom.h"
#include "json/stats.h"
#include "json/selector.h"
#include "module_sim.h"

extern size_t hash_function(const char *, size_t);

/* Since unit tests run outside of Valkey server, we need to map Valkey'
 * memory management functions to cstdlib functions. */
static void SetupAllocFuncs(size_t numShards) {
    setupValkeyModulePointers();
    //
    // Now setup the KeyTable, the RapidJson library now depends on it
    //
    KeyTable::Config c;
    c.malloc = memory_alloc;
    c.free = memory_free;
    c.hash = hash_function;
    c.numShards = numShards;
    keyTable = new KeyTable(c);
}

class TrapsTest : public ::testing::Test {
 protected:
    void SetUp() override {
        JsonUtilCode rc = jsonstats_init();
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        SetupAllocFuncs(16);
    }

    void TearDown() override {
        delete keyTable;
        keyTable = nullptr;
    }
};

//
// See if we can startup and shutdown with no failures
//
TEST_F(TrapsTest, sanity) {
    void *ptr = dom_alloc(15);
    dom_free(ptr);
}

enum JTYPE {
    JT_BOOLEAN,
    JT_INTEGER,
    JT_SHORT_STRING,
    JT_LONG_STRING,
    JT_SHORT_DOUBLE,
    JT_LONG_DOUBLE,
    JT_ARRAY,
    JT_OBJECT,
    JT_OBJECT_HT,
    JT_NUM_TYPES
};

static void makeValue(JValue *v, JTYPE jt) {
    std::string json;
    switch (jt) {
        case JT_BOOLEAN:
            json = "true";
            break;
        case JT_INTEGER:
            json = "1";
            break;
        case JT_SHORT_STRING:
            json = "\"short\"";
            break;
        case JT_LONG_STRING:
            json = "\"string of length large\"";
            break;
        case JT_SHORT_DOUBLE:
            json = "1.2";
            break;
        case JT_LONG_DOUBLE:
            json = "1.23456789101112";
            break;
        case JT_ARRAY:
            json = "[1,2,3,4,5]";
            break;
        case JT_OBJECT:
            json = "{\"a\":1}";
            break;
        case JT_OBJECT_HT:
            json = "{";
            for (auto s = 0; s < 1000; ++s) {
                if (s != 0) json += ',';
                json += '\"';
                json += std::to_string(s);
                json += "\":1";
            }
            json += '}';
            break;
        default:
            ASSERT_TRUE(0);
    }
    JParser parser;
    *v = parser.Parse(json.c_str(), json.length()).GetJValue();
}

//
// Test that keys properly honor corruption
//
TEST_F(TrapsTest, handle_corruption) {
    for (auto corruption : {CORRUPT_PREFIX, CORRUPT_LENGTH, CORRUPT_SUFFIX}) {
        for (auto jt : {JT_OBJECT, JT_OBJECT_HT}) {
            JValue *v = new JValue;
            makeValue(v, jt);
            auto first = v->MemberBegin();
            auto trap_pointer = &*(first->name);
            memory_corrupt_memory(trap_pointer, corruption);
            //
            // Serialize this object
            //
            rapidjson::StringBuffer oss;
            ASSERT_EXIT(dom_serialize_value(*v, nullptr, oss), testing::ExitedWithCode(1), "Validation Failure");
            //
            // Destruct it
            //
            ASSERT_EXIT(delete v, testing::ExitedWithCode(1), "Validation Failure");
            //
            // Cleanup
            //
            memory_uncorrupt_memory(trap_pointer, corruption);
            delete v;
        }
    }
}

//
// Test out the JValue validate and dump functions
//
TEST_F(TrapsTest, jvalue_validation) {
    std::string json =
            "{ \"a\":1, \"b\":[1,2,\"this is a long string\",\"shortstr\",false,true,1.0,1.23456789012345,null]}";
    JParser parser;
    JValue *v = new JValue;
    *v = parser.Parse(json.c_str(), json.length()).GetJValue();
    std::ostringstream os;
    DumpRedactedJValue(os, *v);
    std::cerr << os.str() << "\n";
    delete v;
}

//
// Test Log Stream
//
TEST_F(TrapsTest, test_log_stream) {
    JValue v, v0;
    v.SetArray();
    v.PushBack(v0, allocator);
    DumpRedactedJValue(v, nullptr, "level");
    std::string log = test_getLogText();
    std::cerr << log;
}
