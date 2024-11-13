#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdint>
#include <cmath>
#include <memory>
#include <vector>
#include <string>
#include <sstream>
#include <gtest/gtest.h>
#include "json/dom.h"
#include "json/selector.h"

extern size_t dummy_malloc_size(void *);
extern void SetupAllocFuncs(size_t numShards);
extern std::string& getReplyString();
extern const char *GetString(ReplyBuffer *b);

class SelectorTest : public ::testing::Test {
 protected:
    const char *store = "{\n"
                        "  \"budget\": 10.00,\n"
                        "  \"favorite\": \"Sword of Honour\",\n"
                        "  \"store\": {\n"
                        "    \"books\": [\n"
                        "      {\n"
                        "        \"category\": \"reference\",\n"
                        "        \"author\": \"Nigel Rees\",\n"
                        "        \"title\": \"Sayings of the Century\",\n"
                        "        \"price\": 8.95\n"
                        "      },\n"
                        "      {\n"
                        "        \"category\": \"fiction\",\n"
                        "        \"author\": \"Evelyn Waugh\",\n"
                        "        \"title\": \"Sword of Honour\",\n"
                        "        \"price\": 12.99,\n"
                        "        \"movies\": [\n"
                        "          {\n"
                        "            \"title\": \"Sword of Honour\",\n"
                        "            \"realisator\": {\n"
                        "              \"first_name\": \"Bill\",\n"
                        "              \"last_name\": \"Anderson\"\n"
                        "            }\n"
                        "          }\n"
                        "        ]\n"
                        "      },\n"
                        "      {\n"
                        "        \"category\": \"fiction\",\n"
                        "        \"author\": \"Herman Melville\",\n"
                        "        \"title\": \"Moby Dick\",\n"
                        "        \"isbn\": \"0-553-21311-3\",\n"
                        "        \"price\": 9\n"
                        "      },\n"
                        "      {\n"
                        "        \"category\": \"fiction\",\n"
                        "        \"author\": \"J. R. R. Tolkien\",\n"
                        "        \"title\": \"The Lord of the Rings\",\n"
                        "        \"isbn\": \"0-395-19395-8\",\n"
                        "        \"price\": 22.99\n"
                        "      }\n"
                        "    ],\n"
                        "    \"bicycle\": {\n"
                        "      \"color\": \"red\",\n"
                        "      \"price\": 19.95\n"
                        "    }\n"
                        "  }\n"
                        "}";

    const char *node_accounts = "{\n"
                                "  \"clientName\": \"jim\",\n"
                                "  \"nameSpace\": \"BobSpace\",\n"
                                "  \"codeName\": \"codeName\",\n"
                                "  \"codeId\": 5555,\n"
                                "  \"codeData\": {\n"
                                "    \"uTaskQueue_CodeData\": [\n"
                                "      {\n"
                                "        \"stuff\": 99\n"
                                "      }\n"
                                "    ]\n"
                                "  },\n"
                                "  \"nodeData\": [\n"
                                "    {\n"
                                "      \"selfNodeId\": 1,\n"
                                "      \"selfAndChildNodeIds\": [\n"
                                "        1,\n"
                                "        2,\n"
                                "        3\n"
                                "      ],\n"
                                "      \"uTaskQueue_NodeData\": [\n"
                                "        {\n"
                                "          \"hidden\": \"1+2+3\",\n"
                                "          \"usercreate\": -1000\n"
                                "        }\n"
                                "      ]\n"
                                "    },\n"
                                "    {\n"
                                "      \"selfNodeId\": 10,\n"
                                "      \"selfAndChildNodeIds\": [\n"
                                "        10,\n"
                                "        11,\n"
                                "        12\n"
                                "      ],\n"
                                "      \"uTaskQueue_NodeData\": [\n"
                                "        {\n"
                                "         \"hidden\": \"10+11+12\",\n"
                                "         \"other_stuff\": 1000\n"
                                "        }\n"
                                "      ]\n"
                                "    }\n"
                                "  ]\n"
                                "}";

    void SetUp() override {
        SetupAllocFuncs(16);
    }

    void TearDown() override {
        delete keyTable;
        keyTable = nullptr;
    }
};

TEST_F(SelectorTest, test_filterExpr_attributeFilter) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.store.books[?(@.isbn)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_expression_part1) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.store.books[?(@.price<10.0)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = dom_set_value(nullptr, d1, "$.store.books[?(@.price<10.0)].price", "10.01");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<10.0)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<=1.01e+1)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price==10.01)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.category==\"fiction\")]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 3);

    rc = selector.getValues(*d1, "$.store.books[?(@.category=='fiction')]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 3);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_expression_part2) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.store.books[?(@.price<9||@.price>10&&@.isbn)].price");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    EXPECT_EQ(rs[0].first->GetDouble(), 8.95);
    EXPECT_EQ(rs[1].first->GetDouble(), 22.99);

    rc = selector.getValues(*d1, "$.store.books[?((@.price<9||@.price>10)&&@.isbn)].price");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    EXPECT_EQ(rs2[0].first->GetDouble(), 22.99);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_expression_part3) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$[\"budget\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    EXPECT_EQ(rs[0].first->GetDouble(), 10.00);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<10.0)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$[\"budget\"])]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$.store.books[1].price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$.store.books[-3].price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$.store.books[+1].price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$['store'][\"books\"][1].price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$.store.[\"books\"][1].price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(@.price<$.['store'].books[1].price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?($['store']..books[1].price>@.price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$[\"favorite\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    EXPECT_STREQ(rs2[0].first->GetString(), "Sword of Honour");

    rc = selector.getValues(*d1, "$.store.books[?(@.title==\"Sword of Honour\")]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?(@.title==$.favorite)].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?(@.title==$[\"favorite\"])].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?(@.title==$.[\"favorite\"])].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?(@.title==$[\"store\"])]");
    EXPECT_NE(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$.store.books[?(@.title==$[\"author\"])]");
    EXPECT_NE(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$.store.books[?(@.title==$[\"nothing\"])]");
    EXPECT_NE(rc, JSONUTIL_SUCCESS);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_expression_part4) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.store.books[?(10.0>@.price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?($.favorite==@.title)].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?($[\"favorite\"]==@.title)].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?($.[\"favorite\"]==@.title)].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$.store.books[?(9>@.price || 10<@.price && @.isbn)].price");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    EXPECT_EQ(rs[0].first->GetDouble(), 8.95);
    EXPECT_EQ(rs[1].first->GetDouble(), 22.99);

    rc = selector.getValues(*d1, "$.store.books[?(9>@.price||10<@.price&&@.isbn)].price");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_EQ(rs2[0].first->GetDouble(), 8.95);
    EXPECT_EQ(rs2[1].first->GetDouble(), 22.99);

    rc = selector.getValues(*d1, "$.store.books[?((9>@.price||10<@.price)&&@.isbn)].price");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 1);
    EXPECT_EQ(rs3[0].first->GetDouble(), 22.99);

    rc = selector.getValues(*d1, "$.store.books[?($[\"budget\"]>=@.price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = dom_set_value(nullptr, d1, "$.store.books[?(10.0>@.price)].price", "10.01");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$.store.books[?(10.0>@.price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    rc = selector.getValues(*d1, "$.store.books[?(1.01e+1>=@.price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    rc = selector.getValues(*d1, "$.store.books[?(10.01==@.price)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 2);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_expression_part5) {
    const char *input = "[1,2,3,4,5]";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.*.[?(@>2)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 3);
    EXPECT_EQ(rs[1].first->GetInt(), 4);
    EXPECT_EQ(rs[2].first->GetInt(), 5);

    rc = selector.getValues(*d1, "$.*.[?(2<@)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 3);
    EXPECT_EQ(rs2[0].first->GetInt(), 3);
    EXPECT_EQ(rs2[1].first->GetInt(), 4);
    EXPECT_EQ(rs2[2].first->GetInt(), 5);

    rc = selector.getValues(*d1, "$.*.[?(2<@&&@<5)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 2);
    EXPECT_EQ(rs3[0].first->GetInt(), 3);
    EXPECT_EQ(rs3[1].first->GetInt(), 4);

    const char *input2 = "[true,false,true]";
    JDocument *d2;
    rc = dom_parse(nullptr, input2, strlen(input2), &d2);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d2, "$..[?(@==true)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs4 = selector.getResultSet();
    EXPECT_EQ(rs4.size(), 2);
    EXPECT_EQ(rs4[0].first->GetBool(), true);
    EXPECT_EQ(rs4[1].first->GetBool(), true);

    rc = selector.getValues(*d2, "$..[?(@==false)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs5 = selector.getResultSet();
    EXPECT_EQ(rs5.size(), 1);
    EXPECT_EQ(rs5[0].first->GetBool(), false);

    rc = selector.getValues(*d2, "$..[?(@!=false)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs6 = selector.getResultSet();
    EXPECT_EQ(rs6.size(), 2);
    EXPECT_EQ(rs6[0].first->GetBool(), true);
    EXPECT_EQ(rs6[1].first->GetBool(), true);

    rc = selector.getValues(*d2, "$.*.[?(@!=true)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs7 = selector.getResultSet();
    EXPECT_EQ(rs7.size(), 1);
    EXPECT_EQ(rs7[0].first->GetBool(), false);

    rc = selector.getValues(*d2, "$..[?(@>=true)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs8 = selector.getResultSet();
    EXPECT_EQ(rs8.size(), 2);
    EXPECT_EQ(rs8[0].first->GetBool(), true);
    EXPECT_EQ(rs8[1].first->GetBool(), true);

    rc = selector.getValues(*d2, "$..[?(@ <= true)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs9 = selector.getResultSet();
    EXPECT_EQ(rs9.size(), 3);
    EXPECT_EQ(rs9[0].first->GetBool(), true);
    EXPECT_EQ(rs9[1].first->GetBool(), false);
    EXPECT_EQ(rs9[2].first->GetBool(), true);

    rc = selector.getValues(*d2, "$..[?(@>false)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs10 = selector.getResultSet();
    EXPECT_EQ(rs10.size(), 2);
    EXPECT_EQ(rs10[0].first->GetBool(), true);
    EXPECT_EQ(rs10[1].first->GetBool(), true);

    rc = selector.getValues(*d2, "$..[?(@<true)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs11 = selector.getResultSet();
    EXPECT_EQ(rs11.size(), 1);
    EXPECT_EQ(rs11[0].first->GetBool(), false);

    dom_free_doc(d1);
    dom_free_doc(d2);
}

TEST_F(SelectorTest, test_filterExpr_expression_part6) {
    const char *input = "[{\"NumEntry\":1},{\"NumEntry\":2},{\"NumEntry\":3},"
                         "{\"NumEntry\":4},{\"NumEntry\":5},{\"NumEntry\":6}]";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    Selector selector;

    rc = selector.getValues(*d1, "$..[?(@.NumEntry>4)].NumEntry");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs0 = selector.getResultSet();
    EXPECT_EQ(rs0.size(), 2);
    EXPECT_EQ(rs0[0].first->GetInt(), 5);
    EXPECT_EQ(rs0[1].first->GetInt(), 6);

    rc = selector.getValues(*d1, "$..[?(4<@.NumEntry||@.NumEntry<3)].NumEntry");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs1 = selector.getResultSet();
    EXPECT_EQ(rs1.size(), 4);
    EXPECT_EQ(rs1[0].first->GetInt(), 5);
    EXPECT_EQ(rs1[1].first->GetInt(), 6);
    EXPECT_EQ(rs1[2].first->GetInt(), 1);
    EXPECT_EQ(rs1[3].first->GetInt(), 2);

    rc = selector.getValues(*d1, "$..NumEntry[?(@>4)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_EQ(rs2[0].first->GetInt(), 5);
    EXPECT_EQ(rs2[1].first->GetInt(), 6);

    rc = selector.getValues(*d1, "$..[\"NumEntry\"][?(6>@&&@>3)]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 2);
    EXPECT_EQ(rs3[0].first->GetInt(), 4);
    EXPECT_EQ(rs3[1].first->GetInt(), 5);

    rc = selector.getValues(*d1, "$..NumEntry[?(@.NumEntry)]");
    EXPECT_EQ(rc, JSONUTIL_INVALID_JSON_PATH);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_expression_part7) {
    const char *input = "{"
                        "  \"key for key\"     : \"key inside here\","
                        "  \"key$for$key\"     : \"key inside here\","
                        "  \"key'for'key\"     : \"key inside here\","
                        "  \"key\\\"for\\\"key\"     : \"key inside here\","
                        "  \"an object\"       : {"
                        "    \"weight\"   : 300,"
                        "    \"a value\"  : 300,"
                        "    \"poquo value\"  : \"\\\"\","
                        "    \"my key\"   : \"key inside here\""
                        "  },"
                        "  \"anonther object\" : {"
                        "    \"weight\"   : 400,"
                        "    \"a value\"  : 400,"
                        "    \"poquo value\"  : \"'\","
                        "    \"my key\"   : \"key inside there\""
                        "  }"
                        "}";

    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==\"key inside here\")]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$[\"key for key\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==$[\"key for key\"])]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);

    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==$[\"key$for$key\"])].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    EXPECT_EQ(rs[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==$[\"key'for'key\"])].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    EXPECT_EQ(rs2[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==$[\"key\\\"for\\\"key\"])].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 1);
    EXPECT_EQ(rs3[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==$[\"key for key\"])].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs4 = selector.getResultSet();
    EXPECT_EQ(rs4.size(), 1);
    EXPECT_EQ(rs4[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$..[?($[\"key for key\"]==@[\"my key\"])].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs5 = selector.getResultSet();
    EXPECT_EQ(rs5.size(), 1);
    EXPECT_EQ(rs5[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$..[?(@[\"poquo value\"]=='\"')].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs6 = selector.getResultSet();
    EXPECT_EQ(rs6.size(), 1);
    EXPECT_EQ(rs6[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$..[?(@[\"poquo value\"]==\"'\")].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs7 = selector.getResultSet();
    EXPECT_EQ(rs7.size(), 1);
    EXPECT_EQ(rs7[0].first->GetInt(), 400);

    rc = selector.getValues(*d1, "$..[?(@[\"poquo value\"]=='\\\'')].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs8 = selector.getResultSet();
    EXPECT_EQ(rs8.size(), 1);
    EXPECT_EQ(rs8[0].first->GetInt(), 400);

    rc = selector.getValues(*d1, "$..[?(@[\"my key\"]==$.\"key'for'key\")].weight");
    EXPECT_NE(rc, JSONUTIL_SUCCESS);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filterExpr_single_recursion_array) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, node_accounts, strlen(node_accounts), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[?(@==10)])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs0 = selector.getResultSet();
    EXPECT_EQ(rs0.size(), 1);
    EXPECT_STREQ(rs0[0].first->GetString(), "10+11+12");

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[?(2==@)])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs1 = selector.getResultSet();
    EXPECT_EQ(rs1.size(), 1);
    EXPECT_STREQ(rs1[0].first->GetString(), "1+2+3");

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[?(100>=@)])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_STREQ(rs2[0].first->GetString(), "1+2+3");
    EXPECT_STREQ(rs2[1].first->GetString(), "10+11+12");

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[?(100<=@)])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 0);

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[?(@<10)])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs4 = selector.getResultSet();
    EXPECT_EQ(rs4.size(), 1);
    EXPECT_STREQ(rs4[0].first->GetString(), "1+2+3");

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[?(@<11)])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs5 = selector.getResultSet();
    EXPECT_EQ(rs5.size(), 2);
    EXPECT_STREQ(rs5[0].first->GetString(), "1+2+3");
    EXPECT_STREQ(rs5[1].first->GetString(), "10+11+12");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filertExpr_array_index_single_recursion) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, node_accounts, strlen(node_accounts), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[0]==10)]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs0 = selector.getResultSet();
    EXPECT_EQ(rs0.size(), 1);
    EXPECT_STREQ(rs0[0].first->GetString(), "10+11+12");

    rc = selector.getValues(*d1, "$..nodeData[?(2==@.selfAndChildNodeIds[1])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs1 = selector.getResultSet();
    EXPECT_EQ(rs1.size(), 1);
    EXPECT_STREQ(rs1[0].first->GetString(), "1+2+3");

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[0]>0)]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_STREQ(rs2[0].first->GetString(), "1+2+3");
    EXPECT_STREQ(rs2[1].first->GetString(), "10+11+12");

    rc = selector.getValues(*d1, "$..nodeData[?(-5>=@.selfAndChildNodeIds[0])]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 0);

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[-1]==3)]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs4 = selector.getResultSet();
    EXPECT_EQ(rs4.size(), 1);
    EXPECT_STREQ(rs4[0].first->GetString(), "1+2+3");

    rc = selector.getValues(*d1, "$..nodeData[?(@.selfAndChildNodeIds[2]!=17)]..hidden");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs5 = selector.getResultSet();
    EXPECT_EQ(rs5.size(), 2);
    EXPECT_STREQ(rs5[0].first->GetString(), "1+2+3");
    EXPECT_STREQ(rs5[1].first->GetString(), "10+11+12");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveDescent_get_part1) {
    const char *input = "{\"x\": {}, \"y\": {\"a\":\"a\"}, \"z\": {\"a\":\"\", \"b\":\"b\"}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..a");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    EXPECT_STREQ(rs[0].first->GetString(), "a");
    EXPECT_STREQ(rs[1].first->GetString(), "");

    input = "{\"a\":{\"b\":{\"z\":{\"y\":1}}, \"c\":{\"z\":{\"y\":2}}, \"z\":{\"y\":3}}}";
    JDocument *d2;
    rc = dom_parse(nullptr, input, strlen(input), &d2);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d2, "$.a..z.y");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 3);
    EXPECT_EQ(rs2[0].first->GetInt(), 3);
    EXPECT_EQ(rs2[1].first->GetInt(), 1);
    EXPECT_EQ(rs2[2].first->GetInt(), 2);

    rc = selector.getValues(*d1, "$...a");
    EXPECT_EQ(rc, JSONUTIL_INVALID_DOT_SEQUENCE);

    // note explicit check for odd number of dots
    rc = selector.getValues(*d2, "$.a...z.y");
    EXPECT_EQ(rc, JSONUTIL_INVALID_DOT_SEQUENCE);

    // note explicit check for even number of dots
    rc = selector.getValues(*d2, "$.a.z....y");
    EXPECT_EQ(rc, JSONUTIL_INVALID_DOT_SEQUENCE);

    rc = selector.getValues(*d1, "$........a");
    EXPECT_EQ(rc, JSONUTIL_INVALID_DOT_SEQUENCE);

    rc = selector.getValues(*d2, "$.a........z.y");
    EXPECT_EQ(rc, JSONUTIL_INVALID_DOT_SEQUENCE);

    dom_free_doc(d1);
    dom_free_doc(d2);
}

TEST_F(SelectorTest, test_recursiveDescent_get_part2) {
    const char *input = "{\"a\":1, \"b\": {\"e\":[0,1,2]}, \"c\":{\"e\":[10,11,12]}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..e.[*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 6);
    EXPECT_EQ(rs[0].first->GetInt(), 0);
    EXPECT_EQ(rs[1].first->GetInt(), 1);
    EXPECT_EQ(rs[2].first->GetInt(), 2);
    EXPECT_EQ(rs[3].first->GetInt(), 10);
    EXPECT_EQ(rs[4].first->GetInt(), 11);
    EXPECT_EQ(rs[5].first->GetInt(), 12);

    rc = selector.getValues(*d1, "$..e.[1]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_EQ(rs2[0].first->GetInt(), 1);
    EXPECT_EQ(rs2[1].first->GetInt(), 11);

    rc = selector.getValues(*d1, "$..e.[0:2]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 4);
    EXPECT_EQ(rs3[0].first->GetInt(), 0);
    EXPECT_EQ(rs3[1].first->GetInt(), 1);
    EXPECT_EQ(rs3[2].first->GetInt(), 10);
    EXPECT_EQ(rs3[3].first->GetInt(), 11);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveDescent_get_part3) {
    const char *input = "{\"a\":1, \"b\": {\"e\":[0,1,2]}, \"c\":{\"e\":[10,11,12]}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..e[*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 6);
    EXPECT_EQ(rs[0].first->GetInt(), 0);
    EXPECT_EQ(rs[1].first->GetInt(), 1);
    EXPECT_EQ(rs[2].first->GetInt(), 2);
    EXPECT_EQ(rs[3].first->GetInt(), 10);
    EXPECT_EQ(rs[4].first->GetInt(), 11);
    EXPECT_EQ(rs[5].first->GetInt(), 12);

    rc = selector.getValues(*d1, "$..e[1]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_EQ(rs2[0].first->GetInt(), 1);
    EXPECT_EQ(rs2[1].first->GetInt(), 11);

    rc = selector.getValues(*d1, "$..e[0:2]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 4);
    EXPECT_EQ(rs3[0].first->GetInt(), 0);
    EXPECT_EQ(rs3[1].first->GetInt(), 1);
    EXPECT_EQ(rs3[2].first->GetInt(), 10);
    EXPECT_EQ(rs3[3].first->GetInt(), 11);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveDescent_get_part4) {
    const char *input = "{\"a\":1, \"b\": {\"e\":[0,1,2]}, \"c\":{\"e\":[10,11,12]}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..[\"e\"][*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 6);
    EXPECT_EQ(rs[0].first->GetInt(), 0);
    EXPECT_EQ(rs[1].first->GetInt(), 1);
    EXPECT_EQ(rs[2].first->GetInt(), 2);
    EXPECT_EQ(rs[3].first->GetInt(), 10);
    EXPECT_EQ(rs[4].first->GetInt(), 11);
    EXPECT_EQ(rs[5].first->GetInt(), 12);

    rc = selector.getValues(*d1, "$..[\"e\"][1]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_EQ(rs2[0].first->GetInt(), 1);
    EXPECT_EQ(rs2[1].first->GetInt(), 11);

    rc = selector.getValues(*d1, "$..[\"e\"][0:2]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 4);
    EXPECT_EQ(rs3[0].first->GetInt(), 0);
    EXPECT_EQ(rs3[1].first->GetInt(), 1);
    EXPECT_EQ(rs3[2].first->GetInt(), 10);
    EXPECT_EQ(rs3[3].first->GetInt(), 11);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveDescent_get_part5) {
    const char *input = "{\"a\":{\"a\":{\"a\":{\"a\":1}}}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..a");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 4);

    rapidjson::StringBuffer sb;
    dom_serialize_value(*rs[0].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "{\"a\":{\"a\":{\"a\":1}}}");
    sb.Clear();
    dom_serialize_value(*rs[1].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "{\"a\":{\"a\":1}}");
    sb.Clear();
    dom_serialize_value(*rs[2].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "{\"a\":1}");
    sb.Clear();
    dom_serialize_value(*rs[3].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "1");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveInsertUpdateDelete) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 5);

    // recursive insert and update
    rc = dom_set_value(nullptr, d1, "$..title", "\"foo\"", false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    rc = selector.getValues(*d1, "$..title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 5);
    rc = selector.getValues(*d1, "$.title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);
    rc = selector.getValues(*d1, "$.store.title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetString(), "foo");
    rc = selector.getValues(*d1, "$.store.books[1].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    rc = selector.getValues(*d1, "$.store.books[1].movies[0].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    rc = selector.getValues(*d1, "$.store.books[1].movies[0].realisator.title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);
    rc = selector.getValues(*d1, "$.store.books[1].movies[0].title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetString(), "foo");

    // recursive delete
    size_t num_vals_deleted;
    rc = dom_delete_value(d1, "$..title", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 5);
    rc = selector.getValues(*d1, "$..title");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(selector.getResultSet().empty());

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveInsertUpdateDelete2) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..category");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 4);

    // recursive insert and update
    rc = dom_set_value(nullptr, d1, "$..category", "\"foo\"", false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    rc = selector.getValues(*d1, "$..category");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 4);
    rc = selector.getValues(*d1, "$.category");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);
    rc = selector.getValues(*d1, "$.store.category");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetString(), "foo");
    rc = selector.getValues(*d1, "$.store.books[1].category");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_STREQ(selector.getResultSet()[0].first->GetString(), "foo");

    // recursive delete
    size_t num_vals_deleted;
    rc = dom_delete_value(d1, "$..category", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 4);
    rc = selector.getValues(*d1, "$..category");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(selector.getResultSet().empty());

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_recursiveInsertUpdateDelete3) {
    const char *input = "{\"a\":1, \"b\": {\"e\":[0,1,2]}, \"c\":{\"e\":[10,11,12]}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$..e[*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 6);
    EXPECT_EQ(rs[0].first->GetInt(), 0);
    EXPECT_EQ(rs[1].first->GetInt(), 1);
    EXPECT_EQ(rs[2].first->GetInt(), 2);
    EXPECT_EQ(rs[3].first->GetInt(), 10);
    EXPECT_EQ(rs[4].first->GetInt(), 11);
    EXPECT_EQ(rs[5].first->GetInt(), 12);

    // recursive insert and update
    rc = dom_set_value(nullptr, d1, "$..e[*]", "4", false, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    rc = selector.getValues(*d1, "$..e[*]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(rs.size(), 6);
    EXPECT_EQ(rs[0].first->GetInt(), 4);
    EXPECT_EQ(rs[1].first->GetInt(), 4);
    EXPECT_EQ(rs[2].first->GetInt(), 4);
    EXPECT_EQ(rs[3].first->GetInt(), 4);
    EXPECT_EQ(rs[4].first->GetInt(), 4);
    EXPECT_EQ(rs[5].first->GetInt(), 4);
    rc = selector.getValues(*d1, "$.e");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);
    rc = selector.getValues(*d1, "$.input.e");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    // recursive delete
    size_t num_vals_deleted;
    rc = dom_delete_value(d1, "$..e", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 2);
    rc = selector.getValues(*d1, "$..e");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_TRUE(selector.getResultSet().empty());

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_deep_recursive_update) {
    const char *input = "{\"a\":{\"a\":{\"a\":{\"b\":0}}}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;

    rc = selector.getValues(*d1, "$..b");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_EQ(selector.getResultSet()[0].first->GetInt(), 0);

    rc = dom_set_value(nullptr, d1, "$..b", "1");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$..b");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_EQ(selector.getResultSet()[0].first->GetInt(), 1);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_deep_recursive_update2) {
    const char *input = "{\"a\":{\"a\":{\"a\":{\"a\":{\"z\":\"Z\"}}}}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = dom_set_value(nullptr, d1, "$..a", "\"R\"");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer sb;
    dom_serialize_value(*rs[0].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "{\"a\":\"R\"}");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_deep_recursive_update3) {
    const char *input = "{\"a\":{\"a\":{\"b\":{\"a\":{\"z\":\"Z\"}}}}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = dom_set_value(nullptr, d1, "$..a", "\"R\"");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer sb;
    dom_serialize_value(*rs[0].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "{\"a\":\"R\"}");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_deep_recursive_update4) {
    const char *input = "{\"b\":{\"a\":{\"a\":{\"a\":{\"z\":\"Z\"}}}}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = dom_set_value(nullptr, d1, "$..a", "\"R\"");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d1, "$");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    rapidjson::StringBuffer sb;
    dom_serialize_value(*rs[0].first, nullptr, sb);
    EXPECT_STREQ(sb.GetString(), "{\"b\":{\"a\":\"R\"}}");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filter_on_object) {
    const char *input = "{\"an object\" : {\n"
                        "  \"weight\"  : 300,\n"
                        "  \"a value\" : 300,\n"
                        "  \"my key\"  : \"key inside here\"\n"
                        "}}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.[\"an object\"].[?(@.weight > 200)].[\"a value\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 1);
    EXPECT_EQ(selector.getResultSet()[0].first->GetInt(), 300);

    rc = selector.getValues(*d1, "$.[\"an object\"].[?(@.weight > 300)].[\"a value\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_filter_string_comparison) {
    const char *input = "{\"objects\": ["
                        "    {"
                        "       \"weight\"  : 100,"
                        "       \"a value\" : 100,"
                        "       \"my key\"  : \"key inside here\""
                        "    },"
                        "    {"
                        "       \"weight\"  : 200,"
                        "       \"a value\" : 200,"
                        "       \"my key\"  : \"key inside there\""
                        "    },"
                        "    {"
                        "       \"weight\"  : 300,"
                        "       \"a value\" : 300,"
                        "       \"my key\"  : \"key inside here\""
                        "    },"
                        "    {"
                        "       \"weight\"  : 400,"
                        "       \"a value\" : 400,"
                        "       \"my key\"  : \"key inside there\""
                        "    }"
                        "]}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.[\"objects\"].[?(@.[\"my key\"] == \"key inside there\")].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    EXPECT_EQ(rs[0].first->GetInt(), 200);
    EXPECT_EQ(rs[1].first->GetInt(), 400);

    rc = selector.getValues(*d1, "$.[ \"objects\" ].[?(@.[ \"my key\" ] < \"key inside herf\")].weight");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 2);
    EXPECT_EQ(rs2[0].first->GetInt(), 100);
    EXPECT_EQ(rs2[1].first->GetInt(), 300);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_union_member_names) {
    const char *input = "{\"a\":1, \"b\": 2, \"c\":3}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.[\"a\",\"b\",\"c\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 3);
    EXPECT_EQ(rs[0].first->GetInt(), 1);
    EXPECT_EQ(rs[1].first->GetInt(), 2);
    EXPECT_EQ(rs[2].first->GetInt(), 3);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_malformed_jsonpath) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$[0:2]$[0:1]$[0:2]$[0:2]$[0<2065>:2]$[0:2]");
    EXPECT_EQ(rc, JSONUTIL_JSON_ELEMENT_NOT_ARRAY);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    rc = selector.getValues(*d1, ".[0:2].[0:1].[0:2].[0:2].[0<2065>:2].[0:2]");
    EXPECT_EQ(rc, JSONUTIL_JSON_ELEMENT_NOT_ARRAY);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    rc = selector.getValues(*d1, "$[0,1]");
    EXPECT_EQ(rc, JSONUTIL_JSON_ELEMENT_NOT_ARRAY);
    EXPECT_EQ(selector.getResultSet().size(), 0);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_escaped_controlCharacters) {
    // escaped backslashes, quotes and control characters
    const char *input = "{\"a\\\\a\":1, \"b\\tb\":2, \"c\\nc\":3, \"d\\rd\":4, \"e\\be\":5,"
                        " \"f\\\"f\": 6, \"g g\": 7, \"\": 8, \"\'\":9}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.[\"a\\\\a\",\"b\\tb\",\"c\\nc\",\"d\\rd\","
                            "\"e\\be\",\"f\\\"f\",\"g g\",\"\",\"\'\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 9);
    EXPECT_EQ(rs[0].first->GetInt(), 1);
    EXPECT_EQ(rs[1].first->GetInt(), 2);
    EXPECT_EQ(rs[2].first->GetInt(), 3);
    EXPECT_EQ(rs[3].first->GetInt(), 4);
    EXPECT_EQ(rs[4].first->GetInt(), 5);
    EXPECT_EQ(rs[5].first->GetInt(), 6);
    EXPECT_EQ(rs[6].first->GetInt(), 7);
    EXPECT_EQ(rs[7].first->GetInt(), 8);
    EXPECT_EQ(rs[8].first->GetInt(), 9);

    input = "{\"value_1\": {\"value\" : 10, \"key\": \"linebreak\\n\"}, \"value_2\" : "
            "{\"value\" : 20, \"key\" : \"nolinebreak\"}}";
    JDocument *d2;
    rc = dom_parse(nullptr, input, strlen(input), &d2);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    rc = selector.getValues(*d2, "$..[?(@.key==\"nolinebreak\")].value");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs2 = selector.getResultSet();
    EXPECT_EQ(rs2.size(), 1);
    EXPECT_EQ(rs2[0].first->GetInt(), 20);

    rc = selector.getValues(*d2, "$..[?(@.key=='nolinebreak')].value");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs3 = selector.getResultSet();
    EXPECT_EQ(rs3.size(), 1);
    EXPECT_EQ(rs3[0].first->GetInt(), 20);

    rc = selector.getValues(*d2, "$..[?(@.key==\"linebreak\n\")].value");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs4 = selector.getResultSet();
    EXPECT_EQ(rs4.size(), 0);

    rc = selector.getValues(*d2, "$..[?(@.key=='linebreak\n')].value");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs5 = selector.getResultSet();
    EXPECT_EQ(rs5.size(), 0);

    rc = selector.getValues(*d2, "$..[?(@.key==\"linebreak\\n\")].value");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs6 = selector.getResultSet();
    EXPECT_EQ(rs6.size(), 1);
    EXPECT_EQ(rs6[0].first->GetInt(), 10);

    rc = selector.getValues(*d2, "$..[?(@.key=='linebreak\\n')].value");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs7 = selector.getResultSet();
    EXPECT_EQ(rs7.size(), 1);
    EXPECT_EQ(rs7[0].first->GetInt(), 10);

    dom_free_doc(d2);
    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_escaped_unicode) {
    // escaped unicode
    const char *input = "{\"key\\u0000\":\"value\\\\u0000\", \"key\\u001F\":\"value\\\\u001F\"}";
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, input, strlen(input), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "$.[\"key\\u0000\",\"key\\u001F\"]");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto &rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 2);
    EXPECT_STREQ(rs[0].first->GetString(), "value\\u0000");
    EXPECT_STREQ(rs[1].first->GetString(), "value\\u001F");

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_malformed_query) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    Selector selector;
    rc = selector.getValues(*d1, "&&$.store..price");
    EXPECT_EQ(rc, JSONUTIL_INVALID_MEMBER_NAME);
    EXPECT_TRUE(selector.getResultSet().empty());
    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_delete) {
    JDocument *d1;
    JsonUtilCode rc = dom_parse(nullptr, store, strlen(store), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // delete
    size_t num_vals_deleted;
    rc = dom_delete_value(d1, "$.store.books[?(@.category==\"fiction\")]", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 3);

    dom_free_doc(d1);
}

TEST_F(SelectorTest, test_delete_insert) {
    JDocument *d1;
    const char *json = "{\"a\": { \"b\": { \"c1\": \"abc\", \"c2\": \"foo bar\", \"c3\": \"just a test\" }}}";
    JsonUtilCode rc = dom_parse(nullptr, json, strlen(json), &d1);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // delete
    size_t num_vals_deleted;
    rc = dom_delete_value(d1, "$[\"a\"][\"b\"][\"c2\"]", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 1);

    // insert
    rc = dom_set_value(nullptr, d1, "$[\"a\"][\"b\"][\"c4\"]", "\"good morning\"");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);

    // get
    Selector selector;
    rc = selector.getValues(*d1, "$.a.b.*");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs1 = selector.getResultSet();
    EXPECT_EQ(rs1.size(), 3);

    // delete
    rc = dom_delete_value(d1, "$[\"a\"][\"b\"][*]", num_vals_deleted);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(num_vals_deleted, 3);

    for (int i=0; i < 10; i++) {
        // insert
        rc = dom_set_value(nullptr, d1, "$[\"a\"][\"b\"][\"c\"]", "\"good afternoon\"");
        EXPECT_EQ(rc, JSONUTIL_SUCCESS);

        // delete
        rc = dom_delete_value(d1, "$[\"a\"][\"b\"][*]", num_vals_deleted);
        EXPECT_EQ(rc, JSONUTIL_SUCCESS);
        EXPECT_EQ(num_vals_deleted, 1);
    }

    // get
    rc = selector.getValues(*d1, "$.a.b");
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    auto& rs = selector.getResultSet();
    EXPECT_EQ(rs.size(), 1);
    EXPECT_EQ(rs[0].first->MemberCount(), 0);

    ReplyBuffer oss;
    rc = dom_get_value_as_str(d1, "$", nullptr, oss, false);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_STREQ(GetString(&oss), "[{\"a\":{\"b\":{}}}]");

    dom_free_doc(d1);
}
