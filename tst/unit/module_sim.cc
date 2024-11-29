#undef NDEBUG
#include <assert.h>
#include <stdarg.h>
#include <signal.h>

#include <map>
#include <cstdlib>
#include <cstdio>
#include <ctime>
#include <cstdint>
#include <iostream>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "json/alloc.h"
#include "json/dom.h"
#include "json/stats.h"
#include "json/selector.h"
#include "module_sim.h"

//
// Simulate underlying zmalloc stuff, including malloc-size
//
static std::map<void *, size_t> malloc_sizes;
size_t malloced = 0;
std::string logtext;

static void *test_malloc(size_t s) {
    void *ptr = malloc(s);
    assert(malloc_sizes.find(ptr) == malloc_sizes.end());
    malloc_sizes[ptr] = s;
    malloced += s;
    return ptr;
}

static size_t test_malloc_size(void *ptr) {
    if (!ptr) return 0;
    assert(malloc_sizes.find(ptr) != malloc_sizes.end());
    return malloc_sizes[ptr];
}

static void test_free(void *ptr) {
    if (!ptr) return;
    assert(malloc_sizes.find(ptr) != malloc_sizes.end());
    ASSERT_GE(malloced, malloc_sizes[ptr]);
    malloced -= malloc_sizes[ptr];
    malloc_sizes.erase(malloc_sizes.find(ptr));
    free(ptr);
}

static void *test_realloc(void *old_ptr, size_t new_size) {
    if (old_ptr == nullptr) return test_malloc(new_size);
    assert(malloc_sizes.find(old_ptr) != malloc_sizes.end());
    assert(malloced >= malloc_sizes[old_ptr]);
    malloced -= malloc_sizes[old_ptr];
    malloc_sizes.erase(malloc_sizes.find(old_ptr));
    void *new_ptr = realloc(old_ptr, new_size);
    assert(malloc_sizes.find(new_ptr) == malloc_sizes.end());
    malloc_sizes[new_ptr] = new_size;
    malloced += new_size;
    return new_ptr;
}

std::string test_getLogText() {
    std::string result = logtext;
    logtext.resize(0);
    return result;
}

static void test_log(ValkeyModuleCtx *ctx, const char *level, const char *fmt, ...) {
    (void)ctx;
    char buffer[256];
    va_list arg;
    va_start(arg, fmt);
    int len = vsnprintf(buffer, sizeof(buffer), fmt, arg);
    va_end(arg);
    std::cerr << "Log(" << level << "): " << std::string(buffer, len) << "\n";  // make visible to ASSERT_EXIT
}

static void test__assert(const char *estr, const char *file, int line) {
    ASSERT_TRUE(0) << "Assert(" << file << ":" << line << "): " << estr;
}

static long long test_Milliseconds() {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec * 1000) + (t.tv_nsec / 1000000);
}

void setupValkeyModulePointers() {
    ValkeyModule_Alloc = test_malloc;
    ValkeyModule_Free = test_free;
    ValkeyModule_Realloc = test_realloc;
    ValkeyModule_MallocSize = test_malloc_size;
    ValkeyModule_Log = test_log;
    ValkeyModule__Assert = test__assert;
    ValkeyModule_Strdup = strdup;
    ValkeyModule_Milliseconds = test_Milliseconds;
    memory_traps_control(true);
}

