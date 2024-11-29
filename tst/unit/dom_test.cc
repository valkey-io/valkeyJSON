#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <cmath>
#include <memory>
#include <deque>
#include <string>
#include <sstream>
#include <utility>
#include <iostream>
#include <unordered_map>
#include <map>
#include <gtest/gtest.h>
#include "json/dom.h"
#include "json/alloc.h"
#include "json/stats.h"
#include "json/selector.h"
#include "module_sim.h"

jsn::string& getReplyString() {
    static jsn::string replyString;
    return replyString;
}

static void appendReplyString(const jsn::string& s) {
    jsn::string& rs = getReplyString();
    rs = rs + s;
}

int cs_replyWithBuffer(ValkeyModuleCtx *, const char *buffer, size_t length) {
    appendReplyString(jsn::string(buffer, length));
    return 0;
}

const char *GetString(ReplyBuffer *b) {
    b->Reply();  // Send the string :)
    return getReplyString().c_str();
}

void Clear(rapidjson::StringBuffer *b) {
    b->Clear();
}

void Clear(ReplyBuffer *b) {
    getReplyString().clear();
    b->Clear();
}

size_t cobsize(ValkeyModuleCtx *) {
    return 0;
}

extern size_t hash_function(const char *, size_t);

/* Since unit tests run outside of Valkey server, we need to map Valkey'
 * memory management functions to cstdlib functions. */
void SetupAllocFuncs(size_t numShards) {
    setupValkeyModulePointers();
    //
    // Now setup the KeyTable, the RapidJson library now depends on it
    //
    KeyTable::Config c;
    c.malloc = dom_alloc;
    c.free = dom_free;
    c.hash = hash_function;
    c.numShards = numShards;
    keyTable = new KeyTable(c);
    ValkeyModule_ReplyWithStringBuffer             = cs_replyWithBuffer;
    getReplyString().clear();
}

class DomTest : public ::testing::Test {
 protected:
    const char *json1 = "{"
                            "\"firstName\":\"John\","
                            "\"lastName\":\"Smith\","
                            "\"age\":27,"
                            "\"weight\":135.17,"
                            "\"isAlive\":true,"
                            "\"address\":{"
                                "\"street\":\"21 2nd Street\","
                                "\"city\":\"New York\","
                                "\"state\":\"NY\","
                                "\"zipcode\":\"10021-3100\""
                            "},"
                            "\"phoneNumbers\":["
                                "{"
                                    "\"type\":\"home\","
                                    "\"number\":\"212 555-1234\""
                                "},"
                                "{"
                                    "\"type\":\"office\","
                                    "\"number\":\"646 555-4567\""
                                "}"
                            "],"
                            "\"children\":[],"
                            "\"spouse\":null,"
                            "\"groups\":{}"
                        "}";
    JDocument *doc1;
    const char* json2 = "{"
                            "\"firstName\":\"John\","
                            "\"lastName\":\"Smith\","
                            "\"age\":27,"
                            "\"weight\":135.17,"
                            "\"isAlive\":true,"
                            "\"spouse\":null,"
                            "\"children\":[],"
                            "\"groups\":{}"
                        "}";
    JDocument *doc2;
    const char *json3 = "{"
                        "\"a\":{},"
                        "\"b\":{\"a\":\"a\"},"
                        "\"c\":{\"a\":\"a\", \"b\":1},"
                        "\"d\":{\"a\":\"a\", \"b\":\"b\"},"
                        "\"e\":{\"a\":1, \"b\":\"b\", \"c\":3}"
                        "}";
    JDocument *doc3;
    const char *json4 = "{\"a\":[], \"b\":[1], \"c\":[1,2], \"d\":[1,2,3], \"e\":[1,2,3,4,5]}";
    JDocument *doc4;
    const char *json5 = "{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":{\"f\":{\"g\":{\"h:\":1}}}}}}}}";
    JDocument *doc5;
    const char *json6 = "{"
                        "\"a\":["
                        "[[1,2],[3,4],[5,6]],"
                        "[[7,8],[9,10],[11,12]]"
                        "]"
                        "}";
    JDocument *doc6;

    void SetUp() override {
        JsonUtilCode rc = jsonstats_init();
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        SetupAllocFuncs(16);

        rc = dom_parse(nullptr, json1, strlen(json1), &doc1);
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        rc = dom_parse(nullptr, json2, strlen(json2), &doc2);
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        rc = dom_parse(nullptr, json3, strlen(json3), &doc3);
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        rc = dom_parse(nullptr, json4, strlen(json4), &doc4);
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        rc = dom_parse(nullptr, json5, strlen(json5), &doc5);
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        rc = dom_parse(nullptr, json6, strlen(json6), &doc6);
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
    }

    void TearDown() override {
        dom_free_doc(doc1);
        dom_free_doc(doc2);
        dom_free_doc(doc3);
        dom_free_doc(doc4);
        dom_free_doc(doc5);
        dom_free_doc(doc6);
        delete keyTable;
        keyTable = nullptr;
    }
};

TEST_F(DomTest, testParseObject) {
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, json1, strlen(json1), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    dom_free_doc(doc);
}

TEST_F(DomTest, testParseArray) {
    const char *input = "[1,2,3]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(oss.GetString(), input);

    dom_free_doc(doc);
}

TEST_F(DomTest, testParseString) {
    const char *input = "\"abc\"";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(oss.GetString(), input);

    dom_free_doc(doc);
}

TEST_F(DomTest, testParseNumber) {
    const char *input = "123";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(oss.GetString(), input);

    dom_free_doc(doc);
}

TEST_F(DomTest, testParseBool) {
    const char *input = "false";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(oss.GetString(), input);

    dom_free_doc(doc);
}

TEST_F(DomTest, testParseNull) {
    const char *input = "null";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(oss.GetString(), input);

    dom_free_doc(doc);
}

TEST_F(DomTest, testParseInvalidJSON) {
    const char *input = "{\"a\"}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_JSON_PARSE_ERROR);
    EXPECT_TRUE(doc == nullptr);
}

TEST_F(DomTest, testParseDuplicates) {
    const char *input = "{\"a\":1, \"b\":2, \"a\":3}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    const char *result = "{\"a\":3,\"b\":2}";
    EXPECT_STREQ(oss.GetString(), result);

    dom_free_doc(doc);
}

TEST_F(DomTest, testSerialize_DefaultFormat) {
    rapidjson::StringBuffer oss;
    dom_serialize(doc1, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), json1);
}

TEST_F(DomTest, testSerialize_CustomFormatArray) {
    PrintFormat format;
    format.newline = "\n";
    format.indent  = "\t";
    format.space = ".";
    jsn::vector<std::pair<jsn::string, jsn::string>> tests{
            {"[]", "[]"},
            {"[0]", "[\n\t0\n]"},
            {"[0,1]", "[\n\t0,\n\t1\n]"},
            {"[[]]",  "[\n\t[]\n]"},
            {"[[0]]", "[\n\t[\n\t\t0\n\t]\n]"},
            {"[[0,1]]", "[\n\t[\n\t\t0,\n\t\t1\n\t]\n]"},
            {"{}", "{}"},
            {"{\"a\":0}", "{\n\t\"a\":.0\n}"},
            {"{\"a\":0,\"b\":1}", "{\n\t\"a\":.0,\n\t\"b\":.1\n}"},
            {"{\"a\":{\"b\":1}}", "{\n\t\"a\":.{\n\t\t\"b\":.1\n\t}\n}"}
    };
    for (auto p : tests) {
        JDocument *doc;
        JsonUtilCode rc = dom_parse(nullptr, p.first.c_str(), p.first.length(), &doc);
        EXPECT_EQ(rc, JSONUTIL_SUCCESS);

        rapidjson::StringBuffer oss;
        dom_serialize(doc, &format, oss);
        EXPECT_EQ(rc, JSONUTIL_SUCCESS);
        EXPECT_EQ(oss.GetString(), p.second);
        dom_free_doc(doc);
    }
}

TEST_F(DomTest, testSerialize_CustomFormat) {
    PrintFormat format;
    format.indent = "\t";
    format.newline = "\n";
    format.space = " ";
    const char* exp_json = "{\n\t\"firstName\": \"John\",\n\t\"lastName\": \"Smith\",\n\t\"age\": 27,"
                           "\n\t\"weight\": 135.17,\n\t\"isAlive\": true,\n\t\"spouse\": null,"
                           "\n\t\"children\": [],\n\t\"groups\": {}\n}";
    rapidjson::StringBuffer oss;
    dom_serialize(doc2, &format, oss);
    EXPECT_STREQ(oss.GetString(), exp_json);

    format.indent = "**";
    format.newline = "\n";
    format.space = "--";
    exp_json = "{\n**\"firstName\":--\"John\",\n**\"lastName\":--\"Smith\",\n**\"age\":--27,"
               "\n**\"weight\":--135.17,\n**\"isAlive\":--true,\n**\"spouse\":--null,"
               "\n**\"children\":--[],\n**\"groups\":--{}\n}";
    Clear(&oss);
    dom_serialize(doc2, &format, oss);
    EXPECT_STREQ(oss.GetString(), exp_json);
}

TEST_F(DomTest, testSetString) {
    const char *new_val = "\"Boston\"";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".address.city", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".address.city", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), new_val);
}

TEST_F(DomTest, testSetNumber) {
    const char *new_val = "37";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".age", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".age", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), new_val);
}

TEST_F(DomTest, testSetNull) {
    const char *new_val = "null";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".address.street", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".address.street", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), new_val);
}

TEST_F(DomTest, testSet_NX_XX_ErrorConditions) {
    // Test NX error condition
    const char *new_val = "123";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".firstName", new_val, true, false);
    EXPECT_EQ(rc, JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED);

    // Test XX error condition
    rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, true);
    EXPECT_EQ(rc, JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED);

    // NX and XX must be mutually exclusive
    rc = dom_set_value(nullptr, doc1, ".firstName", new_val, true, true);
    EXPECT_EQ(rc, JSONUTIL_NX_XX_SHOULD_BE_MUTUALLY_EXCLUSIVE);
}

TEST_F(DomTest, testGet_ErrorConditions) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".bar", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_JSON_PATH_NOT_EXIST);
    EXPECT_EQ(strlen(GetString(&oss)), 0);
}

TEST_F(DomTest, testUnicode) {
    const char *new_val = "\"hyvää-élève\"";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".firstName", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".firstName", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), new_val);
}

TEST_F(DomTest, testGetString) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".address.city", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "\"New York\"");

    Clear(&oss);
    rc = dom_get_value_as_str(doc1, ".phoneNumbers[1].number", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "\"646 555-4567\"");
}

TEST_F(DomTest, testGetNumber) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".age", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "27");
}

TEST_F(DomTest, testGetBool) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".isAlive", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "true");
}

TEST_F(DomTest, testGetNull) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".spouse", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "null");
}

TEST_F(DomTest, testGetObject) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".address", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "{\"street\":\"21 2nd Street\",\"city\":\"New York\",\"state\":\"NY\","
    "\"zipcode\":\"10021-3100\"}");
}

TEST_F(DomTest, testGetArray) {
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_value_as_str(doc1, ".phoneNumbers", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[{\"type\":\"home\",\"number\":\"212 555-1234\"},{\"type\":\"office\","
    "\"number\":\"646 555-4567\"}]");

    Clear(&oss);
    rc = dom_get_value_as_str(doc1, ".children", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[]");
}

TEST_F(DomTest, testGet_multiPaths) {
    const char *paths[] = { ".firstName", ".lastName" };
    ReplyBuffer oss;
    JsonUtilCode rc = dom_get_values_as_str(doc1, paths, 2, nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "{\".firstName\":\"John\",\".lastName\":\"Smith\"}");

    // Test pretty print
    PrintFormat format;
    format.indent = "\t";
    format.newline = "\n";
    format.space = " ";
    const char* exp_json = "{\n\t\".firstName\": \"John\",\n\t\".lastName\": \"Smith\"\n}";
    Clear(&oss);
    rc = dom_get_values_as_str(doc1, paths, 2, &format, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), exp_json);
}

TEST_F(DomTest, testDelete) {
    size_t num_vals_deleted;
    JsonUtilCode rc = dom_delete_value(doc1, ".spouse", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 1);
    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".spouse", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_JSON_PATH_NOT_EXIST);
    EXPECT_EQ(oss.GetLength(), 0);

    rc = dom_delete_value(doc1, ".phoneNumbers", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 1);
    Clear(&oss);
    rc = dom_get_value_as_str(doc1, ".phoneNumbers", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_JSON_PATH_NOT_EXIST);
    EXPECT_EQ(oss.GetLength(), 0);
}

TEST_F(DomTest, testDelete_v2path) {
    const char *input = "{\"x\": {}, \"y\": {\"a\":\"a\"}, \"z\": {\"a\":\"\", \"b\":\"b\"}}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    size_t num_vals_deleted;
    rc = dom_delete_value(doc, "$.z.*", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 2);
    const char *exp = "{\"x\":{},\"y\":{\"a\":\"a\"},\"z\":{}}";
    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), exp);

    rc = dom_delete_value(doc, "$.*", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 3);
    exp = "{}";
    Clear(&oss);
    rc = dom_get_value_as_str(doc, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), exp);

    dom_free_doc(doc);
}

TEST_F(DomTest, testDelete_v2path_array) {
    const char *input = "[0,1,2,3,4,5,6,7,8,9]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    size_t num_vals_deleted;
    rc = dom_delete_value(doc, "$[6:10]", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 4);
    const char *exp = "[0,1,2,3,4,5]";
    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), exp);

    rc = dom_delete_value(doc, "$[*]", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 6);
    exp = "[]";
    Clear(&oss);
    rc = dom_get_value_as_str(doc, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), exp);

    dom_free_doc(doc);
}

TEST_F(DomTest, testNumIncrBy_int) {
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    JsonUtilCode rc = dom_increment_by(doc1, ".age", &parser.Parse("1", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 28);
    EXPECT_FALSE(isV2Path);

    rc = dom_increment_by(doc1, ".age", &parser.Parse("-5", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 23);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_float1) {
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    JsonUtilCode rc = dom_increment_by(doc1, ".age", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 27.5);
    EXPECT_FALSE(isV2Path);

    rc = dom_increment_by(doc1, ".age", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 28);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_float2) {
    const char *new_val = "1";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 1.5);
    EXPECT_FALSE(isV2Path);

    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 2);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_int64_overflow) {
    const char *new_val = "9223372036854775807";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(isV2Path);

    // The result exceeds max_int64, and is converted to a double number.
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("1", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 9223372036854775808.0);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("12", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 9223372036854775820.0);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("12", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 9223372036854775832.0);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_int64_overflow_negative) {
    const char *new_val = "-9223372036854775808";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], INT64_MIN);
    EXPECT_FALSE(isV2Path);

    // The result exceeds min_int64, but is converted to a double number.
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("-1", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -9223372036854775809.0);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("-11", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -9223372036854775820.0);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("-11", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -9223372036854775831.0);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_double_overflow) {
    const char *new_val = "1.7e308";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], 1.7e308);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("1.0", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], 1.7e308);
    EXPECT_FALSE(isV2Path);

    // should overflow
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("1.7e308", 7).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0.85e308", 8).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_double_overflow_negative) {
    const char *new_val = "-1.7e308";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -1.7e308);
    EXPECT_FALSE(isV2Path);
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("-1.0", 4).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -1.7e308);
    EXPECT_FALSE(isV2Path);

    // should overflow
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("-1.7e308", 8).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumIncrBy_string_value) {
    Selector selector;
    const char *new_val = "-1.5e308";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    rc = selector.getValues(*doc1, ".foo");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetDoubleString(), "-1.5e308");

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("0", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -1.5e308);

    rc = selector.getValues(*doc1, ".foo");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    // +0 is not a true no-op, because it will re-calculate and reformat a double
    EXPECT_STREQ(selector.getResultSet()[0].first->GetDoubleString(), "-1.5e+308");
}

TEST_F(DomTest, testNumIncrMultBy_string_value_overflow) {
    Selector selector;
    const char *new_val = "-1.7e308";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    rc = selector.getValues(*doc1, ".foo");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetDoubleString(), "-1.7e308");

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, ".foo", &parser.Parse("-1", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -1.7e308);

    rc = selector.getValues(*doc1, ".foo");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetDoubleString(), "-1.6999999999999999e+308");
    //
    // should not overflow
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], -8.5e307);

    rc = selector.getValues(*doc1, ".foo");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetDoubleString(), "-8.4999999999999997e+307");

    // should overflow
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1.0e300", 7).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1.7e308", 7).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testToggle) {
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foobool", "true", false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int> vec;
    bool is_v2_path;
    rc = dom_toggle(doc1, ".foobool", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    rc = dom_toggle(doc1, ".foobool", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    // test a non-array
    rc = dom_set_value(nullptr, doc1, ".foostr", "\"ok\"", false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = dom_toggle(doc1, ".foostr", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_JSON_ELEMENT_NOT_BOOL);
}

TEST_F(DomTest, testToggle_v2path) {
    const char *input1 = "[true, false, 1, null, \"foo\", [], {}]";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input1, strlen(input1), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int> vec;
    bool is_v2_path;
    rc = dom_toggle(d1, "$[*]", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 7);
    EXPECT_EQ(vec[0], 0);
    EXPECT_EQ(vec[1], 1);
    EXPECT_EQ(vec[2], -1);
    EXPECT_EQ(vec[3], -1);
    EXPECT_EQ(vec[4], -1);
    EXPECT_EQ(vec[5], -1);
    EXPECT_EQ(vec[6], -1);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(d1, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[false,true,1,null,\"foo\",[],{}]");

    dom_free_doc(d1);
}

TEST_F(DomTest, testNumMutiBy_int64) {
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    JsonUtilCode rc = dom_multiply_by(doc1, ".age", &parser.Parse("10", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], 270);
    EXPECT_FALSE(isV2Path);

    rc = dom_multiply_by(doc1, ".age", &parser.Parse("10", 2).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], 2700);
    EXPECT_FALSE(isV2Path);

    rc = dom_multiply_by(doc1, ".age", &parser.Parse("0.01", 4).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], 27);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumMutiBy_double) {
    const char *new_val = "1";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 0.5);
    EXPECT_FALSE(isV2Path);

    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("0.5", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 0.25);
    EXPECT_FALSE(isV2Path);

    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("4", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], 1);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumMutiBy_int64_overflow) {
    const char *new_val = "9223372036854775800";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res[0], INT64_MAX);
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("2", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("INT64_MAX", 9).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(isV2Path);

    // should overflow
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1.0e300", 7).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1.7e308", 7).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumMutiBy_int64_overflow_negative) {
    const char *new_val = "-9223372036854775808";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], INT64_MIN);
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("2", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("INT64_MAX", 9).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_FALSE(isV2Path);

    // should overflow
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("-0.85e308", 9).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_TRUE(res.empty());
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("-1.7e308", 8).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_TRUE(res.empty());
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumMutiBy_double_overflow) {
    const char *new_val = "1.7e308";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1.0", 3).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], 1.7e308);
    EXPECT_FALSE(isV2Path);

    // should overflow
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("0.85e308", 8).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_TRUE(res.empty());
    EXPECT_FALSE(isV2Path);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("1.7e308", 7).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_TRUE(res.empty());
    EXPECT_FALSE(isV2Path);
}

TEST_F(DomTest, testNumMutiBy_double_overflow_negative) {
    const char *new_val = "1.7e308";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".foo", new_val, false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should not overflow
    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("-1.0", 4).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], -1.7e308);
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("-1.01", 5).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // should overflow
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("-0.85e308", 9).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_TRUE(res.empty());
    rc = dom_multiply_by(doc1, ".foo", &parser.Parse("-1.7e308", 8).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    EXPECT_TRUE(res.empty());
}

TEST_F(DomTest, testStrLen) {
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_string_length(doc1, ".address.state", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 2);

    rc = dom_string_length(doc1, ".firstName", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 4);
}

TEST_F(DomTest, testStrLen_v2path) {
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_string_length(doc1, "$.address.state", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 2);

    rc = dom_string_length(doc1, "$.firstName", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 4);
}

TEST_F(DomTest, testStrLen_v2path_wildcard) {
    const char *input = "{\"x\": {\"a\":\"\", \"b\":\"b\", \"c\":\"cc\", \"d\":\"ddd\"}}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_string_length(doc, "$.x.*", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 4);
    for (size_t i=0; i < vec.size(); i++) {
        EXPECT_EQ(vec[i], i);
    }

    dom_free_doc(doc);
}

TEST_F(DomTest, testStrAppend) {
    const char *s = "\"son\"";
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_string_append(doc1, ".firstName", s, strlen(s), vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 7);

    s = "\" Senior\"";
    rc = dom_string_append(doc1, ".firstName", s, strlen(s), vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 14);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".firstName", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "\"Johnson Senior\"");
}

TEST_F(DomTest, testStrAppend_v2path) {
    const char *s = "\"son\"";
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_string_append(doc1, "$.firstName", s, strlen(s), vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 7);

    s = "\" Senior\"";
    rc = dom_string_append(doc1, "$.firstName", s, strlen(s), vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 14);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".firstName", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "\"Johnson Senior\"");
}

TEST_F(DomTest, testStrAppend_v2path_wildcard) {
    const char *input = "{\"x\": {\"a\":\"\", \"b\":\"b\", \"c\":\"cc\", \"d\":\"ddd\"}}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    const char *s = "\"z\"";
    rc = dom_string_append(doc, "$.x.*", s, strlen(s), vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 4);
    for (size_t i=0; i < vec.size(); i++) {
        EXPECT_EQ(vec[i], i+1);
    }

    dom_free_doc(doc);
}

TEST_F(DomTest, testObjLen) {
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_object_length(doc1, ".address", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 4);
}

TEST_F(DomTest, testObjLen_v2path_wildcard) {
    const char *input = "{\"x\": {}, \"y\": {\"a\":\"a\"}, \"z\": {\"a\":\"\", \"b\":\"b\"}}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_object_length(doc, "$.*", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 3);
    for (size_t i=0; i < vec.size(); i++) {
        EXPECT_EQ(vec[i], i);
    }

    dom_free_doc(doc);
}

TEST_F(DomTest, testObjKeys) {
    jsn::vector<jsn::vector<jsn::string>> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_object_keys(doc1, ".address", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0].size(), 4);
    EXPECT_STREQ(vec[0][0].c_str(), "street");
    EXPECT_STREQ(vec[0][1].c_str(), "city");
    EXPECT_STREQ(vec[0][2].c_str(), "state");
    EXPECT_STREQ(vec[0][3].c_str(), "zipcode");
}

TEST_F(DomTest, testObjKeys_v2path_wildcard) {
    const char *input = "{\"x\": {}, \"y\": {\"a\":\"a\"}, \"z\": {\"a\":\"\", \"b\":\"b\"}}";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<jsn::vector<jsn::string>> vec;
    bool is_v2_path;
    rc = dom_object_keys(doc, "$.*", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 3);
    EXPECT_TRUE(vec[0].empty());
    EXPECT_EQ(vec[1].size(), 1);
    EXPECT_EQ(vec[2].size(), 2);
    EXPECT_STREQ(vec[1][0].c_str(), "a");
    EXPECT_STREQ(vec[2][0].c_str(), "a");
    EXPECT_STREQ(vec[2][1].c_str(), "b");

    rc = dom_object_keys(doc, ".x", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_TRUE(vec[0].empty());

    dom_free_doc(doc);
}

TEST_F(DomTest, testArrLen) {
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_length(doc1, ".children", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    rc = dom_array_length(doc1, ".phoneNumbers", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 2);
}

TEST_F(DomTest, testArrLen_v2path) {
    const char *input = "[ [\"Marry\", \"Bob\", \"Tom\"], [\"Peter\", \"Marry\", \"Carol\"],"
                        "[\"Peter\", \"Jane\"], [] ]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_array_length(doc, "$[*]", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 4);
    EXPECT_EQ(vec[0], 3);
    EXPECT_EQ(vec[1], 3);
    EXPECT_EQ(vec[2], 2);
    EXPECT_EQ(vec[3], 0);

    dom_free_doc(doc);
}

TEST_F(DomTest, testArrAppend) {
    const char *jsons[] = { "\"John\"" };
    size_t json_lens[] = { 6 };
    jsn::vector<size_t> vec;
    bool is_v2_path;

    JsonUtilCode rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 1, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    jsons[0] = "\"Mary\"";
    json_lens[0] = 6;
    rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 1, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 2);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".children", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[\"John\",\"Mary\"]");
}

TEST_F(DomTest, testArrAppend_multiValues) {
    const char *jsons[] = { "\"John\"", "\"Mary\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 6, 5 };
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 3, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 3);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".children", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[\"John\",\"Mary\",\"Tom\"]");
}

TEST_F(DomTest, testArrAppend_v2path) {
    const char *input = "[ [\"Marry\", \"Bob\"], [\"Peter\"], [] ]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    const char *jsons[] = { "\"John\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 5 };
    rc = dom_array_append(nullptr, doc, "$[*]", jsons, json_lens, 2, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 3);
    EXPECT_EQ(vec[0], 4);
    EXPECT_EQ(vec[1], 3);
    EXPECT_EQ(vec[2], 2);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc, "$[*]", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[[\"Marry\",\"Bob\",\"John\",\"Tom\"],[\"Peter\",\"John\",\"Tom\"],"
                                    "[\"John\",\"Tom\"]]");

    dom_free_doc(doc);
}

TEST_F(DomTest, testArrPop) {
    const char *jsons[] = { "\"John\"", "\"Mary\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 6, 5 };
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 3, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 3);

    jsn::vector<rapidjson::StringBuffer> vec_oss;
    rc = dom_array_pop(doc1, ".children", 1, vec_oss, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec_oss.size(), 1);
    EXPECT_STREQ(vec_oss[0].GetString(), "\"Mary\"");
    EXPECT_EQ(vec_oss[0].GetLength(), 6);

    rc = dom_array_pop(doc1, ".children", -1, vec_oss, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec_oss.size(), 1);
    EXPECT_STREQ(vec_oss[0].GetString(), "\"Tom\"");
    EXPECT_EQ(vec_oss[0].GetLength(), 5);

    rc = dom_array_pop(doc1, ".children", 0, vec_oss, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec_oss.size(), 1);
    EXPECT_STREQ(vec_oss[0].GetString(), "\"John\"");
    EXPECT_EQ(vec_oss[0].GetLength(), 6);
}

TEST_F(DomTest, testArrPop_v2path) {
    const char *input = "[ [\"Marry\", \"Bob\", \"Tom\"], [\"Peter\", \"Carol\"], [\"Jane\"], [] ]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<rapidjson::StringBuffer> vec;
    bool is_v2_path;
    rc = dom_array_pop(doc, "$[*]", 0, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 4);
    EXPECT_STREQ(vec[0].GetString(), "\"Marry\"");
    EXPECT_STREQ(vec[1].GetString(), "\"Peter\"");
    EXPECT_STREQ(vec[2].GetString(), "\"Jane\"");
    EXPECT_EQ(vec[3].GetLength(), 0);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc, "$[*]", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[[\"Bob\",\"Tom\"],[\"Carol\"],[],[]]");

    dom_free_doc(doc);
}

TEST_F(DomTest, testArrInsert) {
    const char *vals[1] = {"\"john\""};
    size_t val_lens[1] = { 6 };
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_insert(nullptr, doc1, ".children", 0, vals, val_lens, 1, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1u);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".children", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[\"john\"]");
}

TEST_F(DomTest, testArrInsert_v2path) {
    const char *input = "[ [\"Marry\", \"Bob\"], [\"Peter\"], [] ]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    const char *jsons[] = { "\"John\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 5 };
    rc = dom_array_insert(nullptr, doc, "$[*]", 0, jsons, json_lens, 2, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 3);
    EXPECT_EQ(vec[0], 4);
    EXPECT_EQ(vec[1], 3);
    EXPECT_EQ(vec[2], 2);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc, "$[*]", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[[\"John\",\"Tom\",\"Marry\",\"Bob\"],"
                                    "[\"John\",\"Tom\",\"Peter\"],[\"John\",\"Tom\"]]");

    dom_free_doc(doc);
}

TEST_F(DomTest, testClear) {
    const char *jsons[] = { "\"John\"", "\"Mary\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 6, 5 };
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 3, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 3);

    // return value should be the number of elements deleted, same as above
    size_t containers_cleared;
    rc = dom_clear(doc1, ".children", containers_cleared);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(containers_cleared, 1);

    // zero elements should remain
    rc = dom_array_length(doc1, ".children", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    // clear empty array
    rc = dom_clear(doc1, ".children", containers_cleared);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(containers_cleared, 0);

    rc = dom_array_length(doc1, ".children", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    // clear an object
    rc = dom_object_length(doc1, ".address", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 4);

    rc = dom_clear(doc1, ".address", containers_cleared);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(containers_cleared, 1);

    vec.clear();
    rc = dom_object_length(doc1, ".address", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);
}

TEST_F(DomTest, testClear_v2path) {
    const char *input1 = "{\"a\":{}, \"b\":{\"a\": 1, \"b\": null, \"c\": true}, "
                         "\"c\":1, \"d\":true, \"e\":null, \"f\":\"d\", \"g\": 4, \"h\": 4.5}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input1, strlen(input1), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    size_t elements_cleared;
    rc = dom_clear(d1, "$.*", elements_cleared);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(elements_cleared, 6);  // everything except the null gets cleared

    ReplyBuffer oss;
    rc = dom_get_value_as_str(d1, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "{\"a\":{},\"b\":{},\"c\":0,\"d\":false,\"e\":null,\"f\":\"\",\"g\":0,\"h\":0.0}");

    const char *input2 = "[[], [0], [0,1], [0,1,2], 1, true, null, \"d\"]";
    JDocument *d2;
    rc = dom_parse(nullptr, input2, strlen(input2), &d2);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = dom_clear(d2, "$[*]", elements_cleared);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(elements_cleared, 6);

    Clear(&oss);
    rc = dom_get_value_as_str(d2, ".", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[[],[],[],[],0,false,null,\"\"]");

    dom_free_doc(d1);
    dom_free_doc(d2);
}

TEST_F(DomTest, testArrTrim) {
    const char *jsons[] = { "\"John\"", "\"Mary\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 6, 5 };
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 3, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 3);

    rc = dom_array_trim(doc1, ".children", 1, 2, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 2);

    rc = dom_array_trim(doc1, ".children", 0, 0, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc1, ".children", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[\"Mary\"]");

    rc = dom_array_trim(doc1, ".children", -1, 5, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    Clear(&oss);
    rc = dom_get_value_as_str(doc1, ".children", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[\"Mary\"]");

    rc = dom_array_trim(doc1, ".phoneNumbers", 2, 0, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    rc = dom_array_length(doc1, ".phoneNumbers", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);
}

TEST_F(DomTest, testArrTrim_v2path) {
    const char *input = "[ [\"Marry\", \"Bob\", \"Tom\"], [\"Peter\", \"Carol\"], [\"Jane\"], [] ]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_array_trim(doc, "$[*]", 0, 1, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 4);
    EXPECT_EQ(vec[0], 2);
    EXPECT_EQ(vec[1], 2);
    EXPECT_EQ(vec[2], 1);
    EXPECT_EQ(vec[3], 0);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(doc, "$[*]", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[[\"Marry\",\"Bob\"],[\"Peter\",\"Carol\"],[\"Jane\"],[]]");

    dom_free_doc(doc);
}

TEST_F(DomTest, testArrIndex) {
    const char *jsons[] = { "\"John\"", "\"Marry\"", "\"Tom\"" };
    size_t json_lens[] = { 6, 7, 5 };
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_array_append(nullptr, doc1, ".children", jsons, json_lens, 3, vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec[0], 3);

    jsn::vector<int64_t> indexes;
    rc = dom_array_index_of(doc1, ".children", "\"Marry\"", 7, 0, 2, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 1);

    rc = dom_array_index_of(doc1, ".children", "\"Tom\"", 5, 0, -1, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(indexes.size(), 1);
    EXPECT_EQ(indexes[0], 2);
}

TEST_F(DomTest, testArrIndex_v2path) {
    const char *input = "[ [\"Marry\", \"Bob\", \"Tom\"], [\"Peter\", \"Marry\", \"Carol\"], "
                        "[\"Peter\", \"Jane\"], [] ]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<int64_t> indexes;
    bool is_v2_path;
    rc = dom_array_index_of(doc, "$[*]", "\"Marry\"", 7, 0, 2, indexes, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(indexes.size(), 4);
    EXPECT_EQ(indexes[0], 0);
    EXPECT_EQ(indexes[1], 1);
    EXPECT_EQ(indexes[2], -1);
    EXPECT_EQ(indexes[3], -1);

    dom_free_doc(doc);
}

TEST_F(DomTest, testValueType) {
    jsn::vector<jsn::string> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_value_type(doc1, ".firstName", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "string");

    rc = dom_value_type(doc1, ".age", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "integer");

    rc = dom_value_type(doc1, ".weight", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "number");

    rc = dom_value_type(doc1, ".isAlive", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "boolean");

    rc = dom_value_type(doc1, ".spouse", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "null");

    rc = dom_value_type(doc1, ".children", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "array");

    rc = dom_value_type(doc1, ".phoneNumbers", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "array");

    rc = dom_value_type(doc1, ".", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "object");

    rc = dom_value_type(doc1, ".groups", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_STREQ(vec[0].c_str(), "object");
}

TEST_F(DomTest, testType_v2path) {
    const char *input = "[1, 2.3, \"foo\", true, null, {}, []]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<jsn::string> vec;
    bool is_v2_path;
    rc = dom_value_type(doc, "$[*]", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 7);
    EXPECT_STREQ(vec[0].c_str(), "integer");
    EXPECT_STREQ(vec[1].c_str(), "number");
    EXPECT_STREQ(vec[2].c_str(), "string");
    EXPECT_STREQ(vec[3].c_str(), "boolean");
    EXPECT_STREQ(vec[4].c_str(), "null");
    EXPECT_STREQ(vec[5].c_str(), "object");
    EXPECT_STREQ(vec[6].c_str(), "array");

    dom_free_doc(doc);
}

TEST_F(DomTest, testNumFields) {
    jsn::vector<size_t> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_num_fields(doc1, ".", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 20);

    rc = dom_num_fields(doc1, ".firstName", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    rc = dom_num_fields(doc1, ".age", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    rc = dom_num_fields(doc1, ".isAlive", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    rc = dom_num_fields(doc1, ".spouse", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 1);

    rc = dom_num_fields(doc1, ".groups", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    rc = dom_num_fields(doc1, ".children", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 0);

    rc = dom_num_fields(doc1, ".address", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 4);

    rc = dom_num_fields(doc1, ".phoneNumbers", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_FALSE(is_v2_path);
    EXPECT_EQ(vec.size(), 1);
    EXPECT_EQ(vec[0], 6);
}

TEST_F(DomTest, testNumFields_v2path) {
    const char *input = "[1, 2.3, \"foo\", true, null, {}, [], {\"a\":1, \"b\":2}, [1,2,3]]";
    JDocument *doc;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &doc);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_num_fields(doc, "$[*]", vec, is_v2_path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(is_v2_path);
    EXPECT_EQ(vec.size(), 9);
    EXPECT_EQ(vec[0], 1);
    EXPECT_EQ(vec[1], 1);
    EXPECT_EQ(vec[2], 1);
    EXPECT_EQ(vec[3], 1);
    EXPECT_EQ(vec[4], 1);
    EXPECT_EQ(vec[5], 0);
    EXPECT_EQ(vec[6], 0);
    EXPECT_EQ(vec[7], 2);
    EXPECT_EQ(vec[8], 3);

    dom_free_doc(doc);
}

TEST_F(DomTest, testSelector_get_legacyPath_wildcard) {
    const char *path = ".address.*";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 4);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"21 2nd Street\"");
    Clear(&oss);
    dom_serialize_value(*rs[1].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"New York\"");
    Clear(&oss);
    dom_serialize_value(*rs[2].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"NY\"");
    Clear(&oss);
    dom_serialize_value(*rs[3].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"10021-3100\"");

    path = ".address.city.*";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_INVALID_JSON_PATH);
    EXPECT_TRUE(selector.getResultSet().empty());
}

TEST_F(DomTest, testSelector_get_v2path_wildcard) {
    const char *path = "$.address.*";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    rapidjson::StringBuffer oss;
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 4);
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"21 2nd Street\"");
    Clear(&oss);
    dom_serialize_value(*rs[1].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"New York\"");
    Clear(&oss);
    dom_serialize_value(*rs[2].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"NY\"");
    Clear(&oss);
    dom_serialize_value(*rs[3].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"10021-3100\"");

    path = "$.address.city.*";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_INVALID_USE_OF_WILDCARD);
    EXPECT_TRUE(selector.getResultSet().empty());

    path = "$.address.city";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    Clear(&oss);
    dom_serialize_value(*rs2[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"New York\"");
}

TEST_F(DomTest, testSelector_get_array_legacyPath) {
    const char *path = ".phoneNumbers[0]";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "{\"type\":\"home\",\"number\":\"212 555-1234\"}");

    path = ".phoneNumbers[0].type";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    Clear(&oss);
    dom_serialize_value(*rs2[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"home\"");

    path = ".phoneNumbers[2]";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES);
    EXPECT_TRUE(selector.getResultSet().empty());

    path = ".phoneNumbers[2].number";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES);
    EXPECT_TRUE(selector.getResultSet().empty());

    path = ".phoneNumbers[x]";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_ARRAY_INDEX_NOT_NUMBER);
    EXPECT_TRUE(selector.getResultSet().empty());

    path = ".phoneNumbers[x].number";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_ARRAY_INDEX_NOT_NUMBER);
    EXPECT_TRUE(selector.getResultSet().empty());
}

TEST_F(DomTest, testSelector_get_array_negativeIndex_legacy_and_v2ath) {
    const char *path = ".phoneNumbers[-1]";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "{\"type\":\"office\",\"number\":\"646 555-4567\"}");

    path = "$['phoneNumbers'][-2]['number']";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    Clear(&oss);
    dom_serialize_value(*rs2[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"212 555-1234\"");

    path = "$['phoneNumbers'][-3]['number']";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES);
    EXPECT_TRUE(selector.getResultSet().empty());
}

TEST_F(DomTest, testSelector_get_array_legacyPath_wildcard) {
    const char *path = ".phoneNumbers[*]";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "{\"type\":\"home\",\"number\":\"212 555-1234\"}");

    path = ".phoneNumbers[*].number";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    Clear(&oss);
    dom_serialize_value(*rs2[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"212 555-1234\"");
}

TEST_F(DomTest, testSelector_get_array_v2path_wildcard) {
    const char *path = "$.phoneNumbers[*]";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "{\"type\":\"home\",\"number\":\"212 555-1234\"}");
    Clear(&oss);
    dom_serialize_value(*rs[1].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "{\"type\":\"office\",\"number\":\"646 555-4567\"}");

    path = "$.phoneNumbers[*].number";
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    Clear(&oss);
    dom_serialize_value(*rs2[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"212 555-1234\"");
    Clear(&oss);
    dom_serialize_value(*rs2[1].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), "\"646 555-4567\"");
}

// Test array slice
TEST_F(DomTest, testSelector_get_array_slice_v2path_wildcard) {
    const char *path = "$.e[1:4]";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 2);

    path = "$.e[2:]";
    rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 3);
    EXPECT_EQ(rs2[2].first->GetInt(), 5);

    path = "$.e[:4]";
    rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 4);
    EXPECT_EQ(rs3[3].first->GetInt(), 4);

    path = "$.e[0:5:2]";
    rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs4 = selector.getResultSet();
    EXPECT_EQ(rs4.size(), 3);
    EXPECT_EQ(rs4[1].first->GetInt(), 3);

    path = "$.e[:5:2]";
    rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs5 = selector.getResultSet();
    EXPECT_EQ(rs5.size(), 3);
    EXPECT_EQ(rs5[1].first->GetInt(), 3);

    path = "$.e[4:0:-2]";
    rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs6 = selector.getResultSet();
    EXPECT_EQ(rs6.size(), 2);
    EXPECT_EQ(rs6[0].first->GetInt(), 5);
}

// Test array union
TEST_F(DomTest, testSelector_get_array_union_v2path) {
    const char *path = "$.e[0,2]";
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc4, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    EXPECT_EQ(rs[0].first->GetInt(), 1);
    EXPECT_EQ(rs[1].first->GetInt(), 3);
}

TEST_F(DomTest, testSelector_set_v2path_part1) {
    const char *path = ".address.*";
    const char *new_val = "\"foo\"";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 4);
    for (auto &vInfo : rs) {
        rapidjson::StringBuffer oss;
        dom_serialize_value(*vInfo.first, nullptr, oss);
        EXPECT_STREQ(oss.GetString(), new_val);
    }

    path = ".address.city.foo";
    rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_CANNOT_INSERT_MEMBER_INTO_NON_OBJECT_VALUE);

    path = ".address.foo.city";
    rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_JSON_PATH_NOT_EXIST);
}

TEST_F(DomTest, testSelector_set_v2path_part2) {
    const char *path = "$.phoneNumbers[*].number";
    const char *new_val = "\"123\"";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    for (auto &vInfo : rs) {
        rapidjson::StringBuffer oss;
        dom_serialize_value(*vInfo.first, nullptr, oss);
        EXPECT_STREQ(oss.GetString(), new_val);
    }

    path = "$.phoneNumbers[x].number";
    rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_ARRAY_INDEX_NOT_NUMBER);

    path = "$.phoneNumbers[2].number";
    rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES);
}

TEST_F(DomTest, testSelector_set_v2path_part3) {
    const char *path = "$.phoneNumbers[-1].number";
    const char *new_val = "\"123\"";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*doc1, path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), new_val);

    path = "$.phoneNumbers[-3].number";
    rc = dom_set_value(nullptr, doc1, path, new_val);
    EXPECT_EQ(rc, JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES);
}

TEST_F(DomTest, testSelector_set_v2path_part4) {
    const char *new_val = "\"z\"";
    const char *path1 = "['address']['z']";
    JsonUtilCode rc = dom_set_value(nullptr, doc1, path1, new_val);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*doc1, path1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer oss;
    dom_serialize_value(*rs[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), new_val);

    Clear(&oss);
    ReplyBuffer oss2;
    rc = dom_get_value_as_str(doc1, path1, nullptr, oss2, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss2), new_val);

    const char *path2 = ".address.z";
    rc = selector.getValues(*doc1, path2);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    Clear(&oss);
    dom_serialize_value(*rs2[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), new_val);

    Clear(&oss);
    Clear(&oss2);
    rc = dom_get_value_as_str(doc1, path2, nullptr, oss2, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss2), new_val);

    const char *path3 = "$.address.z";
    rc = selector.getValues(*doc1, path3);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 1);
    Clear(&oss);
    dom_serialize_value(*rs3[0].first, nullptr, oss);
    EXPECT_STREQ(oss.GetString(), new_val);

    const char *exp = "[\"z\"]";
    Clear(&oss2);
    rc = dom_get_value_as_str(doc1, path3, nullptr, oss2, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss2), exp);
}

TEST_F(DomTest, testSelector_v2path_pathDepth) {
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc1, ".address");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 1);

    rc = selector.getValues(*doc1, ".address.city");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 2);

    rc = selector.getValues(*doc1, ".address.*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 2);

    rc = selector.getValues(*doc1, "$.address.*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 2);

    rc = selector.getValues(*doc1, "$.*.*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 2);

    rc = selector.getValues(*doc1, "$.phoneNumbers[*].type");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 3);

    rc = selector.getValues(*doc1, "$.phoneNumbers[*].*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getMaxPathDepth(), 3);
}

TEST_F(DomTest, test_v2path_NumIncrBy1) {
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".k1", "[1, 2, 3]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, "$.k1[*]", &parser.Parse("1", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], 2);
    EXPECT_EQ(res[1], 3);
    EXPECT_EQ(res[2], 4);

    Selector selector;
    rc = selector.getValues(*doc1, "$.k1[*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 2);
    EXPECT_EQ(rs[1].first->GetInt(), 3);
    EXPECT_EQ(rs[2].first->GetInt(), 4);
}

TEST_F(DomTest, test_v2path_NumIncrBy2) {
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".k1", "{\"a\":1, \"b\":2, \"c\":3}");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_increment_by(doc1, "$.k1.*", &parser.Parse("1", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], 2);
    EXPECT_EQ(res[1], 3);
    EXPECT_EQ(res[2], 4);

    Selector selector;
    rc = selector.getValues(*doc1, "$.k1.*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 2);
    EXPECT_EQ(rs[1].first->GetInt(), 3);
    EXPECT_EQ(rs[2].first->GetInt(), 4);
}

TEST_F(DomTest, test_v2path_NumMultBy1) {
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".k1", "[1, 2, 3]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, "$.k1[*]", &parser.Parse("2", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], 2);
    EXPECT_EQ(res[1], 4);
    EXPECT_EQ(res[2], 6);

    Selector selector;
    rc = selector.getValues(*doc1, "$.k1[*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 2);
    EXPECT_EQ(rs[1].first->GetInt(), 4);
    EXPECT_EQ(rs[2].first->GetInt(), 6);
}

TEST_F(DomTest, test_v2path_NumMultBy2) {
    JsonUtilCode rc = dom_set_value(nullptr, doc1, ".k1", "{\"a\":1, \"b\":2, \"c\":3}");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    jsn::vector<double> res;
    bool isV2Path;
    JParser parser;
    rc = dom_multiply_by(doc1, "$.k1.*", &parser.Parse("2", 1).GetJValue(), res, isV2Path);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res.size(), 3);
    EXPECT_EQ(res[0], 2);
    EXPECT_EQ(res[1], 4);
    EXPECT_EQ(res[2], 6);

    Selector selector;
    rc = selector.getValues(*doc1, "$.k1.*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 2);
    EXPECT_EQ(rs[1].first->GetInt(), 4);
    EXPECT_EQ(rs[2].first->GetInt(), 6);
}

class HTTest : public ::testing::Test {
    void SetUp() override {
        JsonUtilCode rc = jsonstats_init();
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        setShards(0x10);
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

TEST_F(HTTest, hashfunc) {
    enum {key_count = 1 << 18, max_dups = 7};
    std::unordered_map<size_t, size_t> hashes;
    for (size_t i = 0; i < key_count; ++i) {
        std::string s = std::to_string(i);
        size_t h = hash_function(s.c_str(), s.size()) & 0x7FFFF;
        hashes[h]++;
    }
    // Now sort by frequency
    std::map<size_t, jsn::set<size_t>> by_frequency;
    for (auto [h, f] : hashes) {
        by_frequency[f].insert(h);
    }
    ASSERT_LE(by_frequency.begin()->first, max_dups);
}

TEST_F(HTTest, HTIngestTest) {
    for (auto num_keys : { 1<<18 }) {
        rapidjson::hashTableStats.reset();
        std::ostringstream os;
        os << '{';
        for (int i = 0; i < num_keys; ++i) {
            if (i != 0) os << ',';
            os << '"' << i << '"' << ':' << i;
        }
        os << '}';

        JDocument *doc;
        EXPECT_EQ(JSONUTIL_SUCCESS, dom_parse(nullptr, os.str().c_str(), os.str().size(), &doc));
        KeyTable::Stats s = keyTable->getStats();
        EXPECT_EQ(s.handles, num_keys);
        EXPECT_EQ(rapidjson::hashTableStats.rehashUp, 0);
        EXPECT_EQ(rapidjson::hashTableStats.rehashDown, 0);
        EXPECT_EQ(rapidjson::hashTableStats.convertToHT, 0);
        EXPECT_EQ(rapidjson::hashTableStats.reserveHT, 1);
        EXPECT_EQ("", keyTable->validate());
        EXPECT_EQ("", validate(doc));
        // Now make a second identical document
        JDocument *doc2;
        std::cerr << "***** Start second parse ****\n";
        EXPECT_EQ(JSONUTIL_SUCCESS, dom_parse(nullptr, os.str().c_str(), os.str().size(), &doc2));
        s = keyTable->getStats();
        EXPECT_EQ(s.rehashes, 0);
        EXPECT_EQ(s.handles, 2*num_keys);
        EXPECT_EQ(rapidjson::hashTableStats.rehashUp, 0);
        EXPECT_EQ(rapidjson::hashTableStats.rehashDown, 0);
        EXPECT_EQ(rapidjson::hashTableStats.convertToHT, 0);
        EXPECT_EQ(rapidjson::hashTableStats.reserveHT, 2);
        EXPECT_EQ("", keyTable->validate());
        EXPECT_EQ("", validate(doc));
        EXPECT_EQ("", validate(doc2));
        EXPECT_EQ(s.size, num_keys);
        dom_free_doc(doc2);
        EXPECT_EQ(keyTable->getStats().handles, num_keys);
        EXPECT_EQ(keyTable->getStats().size, num_keys);
        dom_free_doc(doc);
        EXPECT_EQ(keyTable->getStats().handles, 0);
        EXPECT_EQ(keyTable->getStats().size, 0);
    }
}
