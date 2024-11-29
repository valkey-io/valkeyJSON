#include <gtest/gtest.h>
#include "json/stats.h"

class StatsTest : public ::testing::Test {
};

TEST_F(StatsTest, testFindBucket) {
    EXPECT_EQ(jsonstats_find_bucket(0), 0);
    EXPECT_EQ(jsonstats_find_bucket(200), 0);
    EXPECT_EQ(jsonstats_find_bucket(256), 1);
    EXPECT_EQ(jsonstats_find_bucket(500), 1);
    EXPECT_EQ(jsonstats_find_bucket(1024), 2);
    EXPECT_EQ(jsonstats_find_bucket(2000), 2);
    EXPECT_EQ(jsonstats_find_bucket(4*1024), 3);
    EXPECT_EQ(jsonstats_find_bucket(5000), 3);
    EXPECT_EQ(jsonstats_find_bucket(16*1024), 4);
    EXPECT_EQ(jsonstats_find_bucket(50000), 4);
    EXPECT_EQ(jsonstats_find_bucket(64*1024), 5);
    EXPECT_EQ(jsonstats_find_bucket(100000), 5);
    EXPECT_EQ(jsonstats_find_bucket(256*1024), 6);
    EXPECT_EQ(jsonstats_find_bucket(1000000), 6);
    EXPECT_EQ(jsonstats_find_bucket(1024*1024), 7);
    EXPECT_EQ(jsonstats_find_bucket(4000000), 7);
    EXPECT_EQ(jsonstats_find_bucket(4*1024*1024), 8);
    EXPECT_EQ(jsonstats_find_bucket(5000000), 8);
    EXPECT_EQ(jsonstats_find_bucket(16*1024*1024), 9);
    EXPECT_EQ(jsonstats_find_bucket(20000000), 9);
    EXPECT_EQ(jsonstats_find_bucket(60*1024*1024), 9);
    EXPECT_EQ(jsonstats_find_bucket(64*1024*1024), 10);
    EXPECT_EQ(jsonstats_find_bucket(90000000), 10);
    EXPECT_EQ(jsonstats_find_bucket(1024*1024*1024), 10);
}
