/**
 * This C module is the JSON memory allocator (also called DOM allocator), which wraps around Valkey's built-in
 * allocation functions - ValkeyModule_Alloc, ValkeyModule_Free and ValkeyModule_Realloc. All memory allocations,
 * permanent or transient, should be done through this interface, so that allocated memories are correctly
 * reported to the Valkey engine (MEMORY STATS).
 *
 * Besides correctly reporting memory usage to Valkey, it also provides a facility to track memory usage of JSON
 * objects, so that we can achieve the following:
 * 1. To track total memory allocated to JSON objects. This is done through an atomic global counter. Note that
 *    Valkey engine only reports total memories for all keys, not by key type. This JSON memory allocator overcomes
 *    such deficiency.
 * 2. To track each JSON document object's memory size. This is done through a thread local counter. With the ability
 *    to track individual document's footprint, we can maintain a few interesting histograms that will provide
 *    insights into data distribution and API access patterns.
 */
#ifndef VALKEYJSONMODULE_ALLOC_H_
#define VALKEYJSONMODULE_ALLOC_H_

#include <stddef.h>

#include "json/memory.h"

void *dom_alloc(size_t size);
void dom_free(void *ptr);
void *dom_realloc(void *orig_ptr, size_t new_size);
char *dom_strdup(const char *s);
char *dom_strndup(const char *s, const size_t n);

#endif  // VALKEYJSONMODULE_ALLOC_H_
