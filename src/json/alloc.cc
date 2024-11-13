#include "json/memory.h"
#include "json/alloc.h"
#include "json/stats.h"
#include <cstdio>
#include <cstring>

extern "C" {
#define VALKEYMODULE_EXPERIMENTAL_API
#include <./include/valkeymodule.h>
}

void *dom_alloc(size_t size) {
    void *ptr = memory_alloc(size);
    // actually allocated size may not be same as the requested size
    size_t real_size = memory_allocsize(ptr);
    jsonstats_increment_used_mem(real_size);
    return ptr;
}

void dom_free(void *ptr) {
    size_t size = memory_allocsize(ptr);
    memory_free(ptr);
    jsonstats_decrement_used_mem(size);
}

void *dom_realloc(void *orig_ptr, size_t new_size) {
    // We need to handle the following two edge cases first. Otherwise, the following
    // calculation of the incremented/decremented amount will fail.
    if (new_size == 0 && orig_ptr != nullptr) {
        dom_free(orig_ptr);
        return nullptr;
    }
    if (orig_ptr == nullptr) return dom_alloc(new_size);

    size_t orig_size = memory_allocsize(orig_ptr);
    void *new_ptr = memory_realloc(orig_ptr, new_size);
    // actually allocated size may not be same as the requested size
    size_t real_new_size = memory_allocsize(new_ptr);
    if (real_new_size > orig_size)
        jsonstats_increment_used_mem(real_new_size - orig_size);
    else if (real_new_size < orig_size)
        jsonstats_decrement_used_mem(orig_size - real_new_size);

    return new_ptr;
}

char *dom_strdup(const char *s) {
    size_t size = strlen(s) + 1;
    char *dup = static_cast<char*>(dom_alloc(size));
    strncpy(dup, s, size);
    return dup;
}

char *dom_strndup(const char *s, const size_t n) {
    char *dup = static_cast<char*>(dom_alloc(n + 1));
    strncpy(dup, s, n);
    dup[n] = '\0';
    return dup;
}
