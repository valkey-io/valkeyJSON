#include <stdint.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <gtest/gtest.h>
#include "json/util.h"
#include "json/dom.h"
#include "json/alloc.h"
#include "json/stats.h"
#include "module_sim.h"

extern size_t dummy_malloc_size(void *);

class UtilTest : public ::testing::Test {
 protected:
    void SetUp() override {
        JsonUtilCode rc = jsonstats_init();
        ASSERT_EQ(rc, JSONUTIL_SUCCESS);
        setupValkeyModulePointers();
    }
};

TEST_F(UtilTest, testCodeToMessage) {
    for (JsonUtilCode code=JSONUTIL_SUCCESS; code < JSONUTIL_LAST; code = JsonUtilCode(code + 1)) {
        const char *msg = jsonutil_code_to_message(code);
        EXPECT_TRUE(msg != nullptr);
        if (code == JSONUTIL_SUCCESS || code == JSONUTIL_WRONG_NUM_ARGS ||
            code == JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED) {
            EXPECT_STREQ(msg, "");
        } else {
            EXPECT_GT(strlen(msg), 0);
        }
    }
}

TEST_F(UtilTest, testDoubleToString) {
    double v = 189.31;
    char buf[BUF_SIZE_DOUBLE_JSON];
    size_t len = jsonutil_double_to_string(v, buf, sizeof(buf));
    EXPECT_STREQ(buf, "189.31");
    EXPECT_EQ(len, strlen(buf));
}

TEST_F(UtilTest, testDoubleToStringRapidJson) {
    double v = 189.31;
    char buf[BUF_SIZE_DOUBLE_RAPID_JSON];
    size_t len = jsonutil_double_to_string_rapidjson(v, buf, sizeof(buf));
    EXPECT_STREQ(buf, "189.31");
    EXPECT_EQ(len, strlen(buf));
}

TEST_F(UtilTest, testIsInt64) {
    EXPECT_TRUE(jsonutil_is_int64(0));
    EXPECT_TRUE(jsonutil_is_int64(1));
    EXPECT_TRUE(jsonutil_is_int64(INT8_MAX));
    EXPECT_TRUE(jsonutil_is_int64(INT8_MIN));
    EXPECT_TRUE(jsonutil_is_int64(INT16_MAX));
    EXPECT_TRUE(jsonutil_is_int64(INT16_MIN));
    EXPECT_TRUE(jsonutil_is_int64(INT32_MAX));
    EXPECT_TRUE(jsonutil_is_int64(INT32_MIN));
    EXPECT_TRUE(jsonutil_is_int64(INT64_MAX >> 1));
    EXPECT_TRUE(jsonutil_is_int64(8223372036854775807LL));
    EXPECT_TRUE(jsonutil_is_int64(INT64_MIN));
    EXPECT_FALSE(jsonutil_is_int64(1e28));      // out of range of int64
    EXPECT_FALSE(jsonutil_is_int64(1.7e308));   // out of range of int64
    EXPECT_FALSE(jsonutil_is_int64(-1e28));     // out of range of int64
    EXPECT_FALSE(jsonutil_is_int64(-1.7e308));  // out of range of int64
    EXPECT_TRUE(jsonutil_is_int64(108.0));
    EXPECT_FALSE(jsonutil_is_int64(108.9));
    EXPECT_FALSE(jsonutil_is_int64(108.0000001));
    EXPECT_TRUE(jsonutil_is_int64(-108.0));
    EXPECT_FALSE(jsonutil_is_int64(-108.9));
    EXPECT_FALSE(jsonutil_is_int64(-108.0000001));
}

TEST_F(UtilTest, testMultiplyInt64_overflow) {
    // should not overflow
    int64_t res;
    JsonUtilCode rc = jsonutil_multiply_int64(INT64_MAX, 1, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, INT64_MAX);

    // should overflow
    rc = jsonutil_multiply_int64(INT64_MAX, 2, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_int64(INT64_MAX, INT64_MAX >> 1, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_int64(INT64_MAX, INT64_MAX, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
}

TEST_F(UtilTest, testMultiplyInt64_overflow_negative) {
    // should not overflow
    int64_t res;
    JsonUtilCode rc = jsonutil_multiply_int64(INT64_MIN, 1, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, INT64_MIN);

    // should overflow
    rc = jsonutil_multiply_int64(INT64_MIN, 2, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_int64(INT64_MIN, INT64_MIN >> 1, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_int64(INT64_MIN, INT64_MAX, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_int64(INT64_MIN, INT64_MIN, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
}

TEST_F(UtilTest, testMultiplyDouble) {
    double res;
    JsonUtilCode rc = jsonutil_multiply_double(5e30, 2, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, 1e31);

    rc = jsonutil_multiply_double(5.0e30, 2.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, 1.0e31);
}

TEST_F(UtilTest, testMultiplyDouble_overflow) {
    // should not overflow
    double res;
    JsonUtilCode rc = jsonutil_multiply_double(1.7e308, 1.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, 1.7e308);

    // should overflow
    rc = jsonutil_multiply_double(1.7e308, 2.0, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_double(1.7e308, 1.7e308, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
}

TEST_F(UtilTest, testMultiplyDouble_overflow_negative) {
    // should not overflow
    double res;
    JsonUtilCode rc = jsonutil_multiply_double(-1.7e308, 1.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, -1.7e308);

    // should overflow
    rc = jsonutil_multiply_double(-1.7e308, 2.0, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
    rc = jsonutil_multiply_double(-1.7e308, 1.7e308, &res);
    EXPECT_EQ(rc, JSONUTIL_MULTIPLICATION_OVERFLOW);
}

TEST_F(UtilTest, testAddInt64_overflow) {
    // should not overflow
    int64_t res;
    JsonUtilCode rc = jsonutil_add_int64(INT64_MAX, 0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, INT64_MAX);

    // should overflow
    rc = jsonutil_add_int64(INT64_MAX, 1, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    rc = jsonutil_add_int64(INT64_MAX, INT64_MAX >> 1, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    rc = jsonutil_add_int64(INT64_MAX, INT64_MAX, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
}

TEST_F(UtilTest, testAddInt64_overflow_negative) {
    // should not overflow
    int64_t res;
    JsonUtilCode rc = jsonutil_add_int64(INT64_MIN, 0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, INT64_MIN);

    // should overflow
    rc = jsonutil_add_int64(INT64_MIN, -1, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    rc = jsonutil_add_int64(INT64_MIN, INT64_MIN >> 1, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    rc = jsonutil_add_int64(INT64_MIN, INT64_MIN, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
}

TEST_F(UtilTest, testAddDouble_overflow) {
    // should not overflow
    double res;
    JsonUtilCode rc = jsonutil_add_double(1.7e308, 0.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, 1.7e308);
    rc = jsonutil_add_double(1.7e308, 1.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, 1.7e308);

    // should overflow
    rc = jsonutil_add_double(1.7e308, 0.85e308, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    rc = jsonutil_add_double(1.7e308, 1.7e308, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
}

TEST_F(UtilTest, testAddDouble_overflow_negative) {
    // should not overflow
    double res;
    JsonUtilCode rc = jsonutil_add_double(-1.7e308, 0.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, -1.7e308);
    rc = jsonutil_add_double(-1.7e308, -1.0, &res);
    EXPECT_EQ(rc, JSONUTIL_SUCCESS);
    EXPECT_EQ(res, -1.7e308);

    // should overflow
    rc = jsonutil_add_double(-1.7e308, -0.85e308, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
    rc = jsonutil_add_double(-1.7e308, -1.7e308, &res);
    EXPECT_EQ(rc, JSONUTIL_ADDITION_OVERFLOW);
}
