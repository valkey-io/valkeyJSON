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
#include "json/dom.h"
#include "json/alloc.h"
#include "json/stats.h"
#include "json/selector.h"

extern void SetupAllocFuncs(size_t numShards);

class JsonTest : public ::testing::Test {
    void SetUp() override {
        JsonUtilCode rc = jsonstats_init();
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        SetupAllocFuncs(16);
    }
    void TearDown() override {
        delete keyTable;
        keyTable = nullptr;
    }
    void setShards(size_t numShards) {
        if (keyTable) delete keyTable;
        SetupAllocFuncs(numShards);
    }
};

TEST_F(JsonTest, testArrIndex_fullobjects) {
    const char *input = "[5, 6, {\"a\":\"b\"}, [99,100], [\"c\"]]";

    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int64_t> indexes;
    bool is_v2_path;
    rc = dom_array_index_of(doc, ".", "{\"a\":\"b\"}", 9, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 2);

    rc = dom_array_index_of(doc, ".", "[\"c\"]", 5, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 4);

    rc = dom_array_index_of(doc, ".", "[99,100]", 8, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 3);

    dom_free_doc(doc);
}

TEST_F(JsonTest, testArrIndex_arr) {
    const char *input = "{\"a\":[1,2,[15,50],3], \"nested\": {\"a\": [3,4,[5,5]]}}";

    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int64_t> indexes;
    bool is_v2_path;
    rc = dom_array_index_of(doc, "$..a", "[15,50]", 7, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 2);
    EXPECT_EQ(indexes[0], 2);
    EXPECT_EQ(indexes[1], -1);

    rc = dom_array_index_of(doc, "$..a", "3", 1, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 2);
    EXPECT_EQ(indexes[0], 3);
    EXPECT_EQ(indexes[1], 0);

    rc = dom_array_index_of(doc, "$..a", "[5,5]", 5, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 2);
    EXPECT_EQ(indexes[0], -1);
    EXPECT_EQ(indexes[1], 2);

    rc = dom_array_index_of(doc, "$..a", "35", 2, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 2);
    EXPECT_EQ(indexes[0], -1);
    EXPECT_EQ(indexes[0], -1);

    dom_free_doc(doc);
}

TEST_F(JsonTest, testArrIndex_object) {
    const char *input = "{\"a\":{\"b\":[2,4,{\"a\":4},false,true,{\"b\":false}]}}";

    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int64_t> indexes;
    bool is_v2_path;
    rc = dom_array_index_of(doc, "$.a.b", "{\"a\":4}", 7, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 2);

    rc = dom_array_index_of(doc, "$.a.b", "{\"b\":false}", 11, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 5);

    rc = dom_array_index_of(doc, "$.a.b", "false", 5, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 3);

    rc = dom_array_index_of(doc, "$..a", "{\"a\":4}", 7, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 2);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], INT64_MAX);

    rc = dom_array_index_of(doc, "$..a..", "{\"a\":4}", 7, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 4);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], 2);
    EXPECT_EQ(indexes[2], INT64_MAX);
    EXPECT_EQ(indexes[3], INT64_MAX);

    dom_free_doc(doc);
}

TEST_F(JsonTest, testArrIndex_nested_search) {
    const char *input = "{\"level0\":{\"level1_0\":{\"level2\":"
                         "[1,2,3, [25, [4,5,{\"c\":\"d\"}]]]},"
                         "\"level1_1\":{\"level2\": [[{\"a\":[2,5]}, true, null]]}}}";

    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int64_t> indexes;
    bool is_v2_path;
    rc = dom_array_index_of(doc, "$..level0.level1_0..", "[4,5,{\"c\":\"d\"}]", 15, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], -1);
    EXPECT_EQ(indexes[2], 1);
    EXPECT_EQ(indexes[3], -1);
    EXPECT_EQ(indexes[4], INT64_MAX);

    rc = dom_array_index_of(doc, "$..level0.level1_0..", "[25, [4,5,{\"c\":\"d\"}]]", 21, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], 3);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], -1);
    EXPECT_EQ(indexes[4], INT64_MAX);

    rc = dom_array_index_of(doc, "$..level0.level1_0..", "{\"c\":\"d\"}", 9, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], -1);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], 2);
    EXPECT_EQ(indexes[4], INT64_MAX);

    rc = dom_array_index_of(doc, "$..level0.level1_0..", "[4,5,{\"a\":\"b\"}]", 15, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], -1);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], -1);
    EXPECT_EQ(indexes[4], INT64_MAX);

    rc = dom_array_index_of(doc, "$..level0.level1_1..", "[null,true,{\"a\":[2,5]}]", 23, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], -1);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], INT64_MAX);
    EXPECT_EQ(indexes[4], -1);

    rc = dom_array_index_of(doc, "$..level0.level1_1..", "[{\"a\":[2,5]},true,null]", 23, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], 0);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], INT64_MAX);
    EXPECT_EQ(indexes[4], -1);

    rc = dom_array_index_of(doc, "$..level0.level1_1..", "[{\"a\":[2,5]},true]", 18, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], -1);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], INT64_MAX);
    EXPECT_EQ(indexes[4], -1);

    rc = dom_array_index_of(doc, "$..level0.level1_0..", "[4,{\"c\":\"d\"}]", 13, 0, 0, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 5);
    EXPECT_EQ(indexes[0], INT64_MAX);
    EXPECT_EQ(indexes[1], -1);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], -1);
    EXPECT_EQ(indexes[4], INT64_MAX);

    dom_free_doc(doc);
}
