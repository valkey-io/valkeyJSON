#include "json/stats.h"
#include <pthread.h>
#include <cstring>
#include <atomic>
#include <string>
#include <iostream>
#include <sstream>
extern "C" {
    #define VALKEYMODULE_EXPERIMENTAL_API
    #include <./include/valkeymodule.h>
}

#define STATIC /* decorator for static functions, remove so that backtrace symbols include these */

LogicalStats logical_stats;  // initialize global variable

// Thread local storage (TLS) key for calculating used memory per thread.
static pthread_key_t thread_local_mem_counter_key;

/* JSON statistics struct.
 * Use atomic integers due to possible multi-threading execution of rdb_load and
 * also the overhead of atomic operations are negligible.
 */
typedef struct {
    std::atomic_ullong used_mem;  // global used memory counter
    std::atomic_ullong num_doc_keys;
    std::atomic_ullong max_depth_ever_seen;
    std::atomic_ullong max_size_ever_seen;
    std::atomic_ullong defrag_count;
    std::atomic_ullong defrag_bytes;

    void reset() {
        used_mem = 0;
        num_doc_keys = 0;
        max_depth_ever_seen = 0;
        max_size_ever_seen = 0;
        defrag_count = 0;
        defrag_bytes = 0;
    }
} JsonStats;
static JsonStats jsonstats;

// histograms
#define NUM_BUCKETS (11)
static size_t buckets[] = {
        0, 256, 1024, 4*1024, 16*1024, 64*1024, 256*1024, 1024*1024,
        4*1024*1024, 16*1024*1024, 64*1024*1024, SIZE_MAX
};

// static histogram showing document size distribution
static size_t doc_hist[NUM_BUCKETS];
// dynamic histogram for read operations (JSON.GET and JSON.MGET only)
static size_t read_hist[NUM_BUCKETS];
// dynamic histogram for insert operations (JSON.SET and JSON.ARRINSERT)
static size_t insert_hist[NUM_BUCKETS];
// dynamic histogram for update operations (JSON.SET, JSON.STRAPPEND and JSON.ARRAPPEND)
static size_t update_hist[NUM_BUCKETS];
// dynamic histogram for delete operations (JSON.DEL, JSON.FORGET, JSON.ARRPOP and JSON.ARRTRIM)
static size_t delete_hist[NUM_BUCKETS];

JsonUtilCode jsonstats_init() {
    ValkeyModule_Assert(jsonstats.used_mem == 0);  // Otherwise you'll lose memory accounting
    // Create thread local key. No need to have destructor hook, as the key is created on stack.
    if (pthread_key_create(&thread_local_mem_counter_key, nullptr) != 0)
        return JSONUTIL_FAILED_TO_CREATE_THREAD_SPECIFIC_DATA_KEY;

    jsonstats.reset();
    logical_stats.reset();
    memset(doc_hist, 0, sizeof(doc_hist));
    memset(read_hist, 0, sizeof(read_hist));
    memset(insert_hist, 0, sizeof(insert_hist));
    memset(update_hist, 0, sizeof(update_hist));
    memset(delete_hist, 0, sizeof(delete_hist));
    return JSONUTIL_SUCCESS;
}

int64_t jsonstats_begin_track_mem() {
    return reinterpret_cast<int64_t>(pthread_getspecific(thread_local_mem_counter_key));
}

int64_t jsonstats_end_track_mem(const int64_t begin_val) {
    int64_t end_val = reinterpret_cast<int64_t>(pthread_getspecific(thread_local_mem_counter_key));
    return end_val - begin_val;
}

void jsonstats_increment_used_mem(size_t delta) {
    // update the atomic global counter
    jsonstats.used_mem += delta;

    // update the thread local counter
    int64_t curr_val = reinterpret_cast<int64_t>(pthread_getspecific(thread_local_mem_counter_key));
    pthread_setspecific(thread_local_mem_counter_key, reinterpret_cast<int64_t*>(curr_val + delta));
}

void jsonstats_decrement_used_mem(size_t delta) {
    // update the atomic global counter
    ValkeyModule_Assert(delta <= jsonstats.used_mem);
    jsonstats.used_mem -= delta;

    // update the thread local counter
    int64_t curr_val = reinterpret_cast<int64_t>(pthread_getspecific(thread_local_mem_counter_key));
    pthread_setspecific(thread_local_mem_counter_key, reinterpret_cast<int64_t*>(curr_val - delta));
}

unsigned long long jsonstats_get_used_mem() {
    return jsonstats.used_mem;
}

unsigned long long jsonstats_get_num_doc_keys() {
    return jsonstats.num_doc_keys;
}

unsigned long long jsonstats_get_max_depth_ever_seen() {
    return jsonstats.max_depth_ever_seen;
}

void jsonstats_update_max_depth_ever_seen(const size_t max_depth) {
    if (max_depth > jsonstats.max_depth_ever_seen) {
        jsonstats.max_depth_ever_seen = max_depth;
    }
}

unsigned long long jsonstats_get_max_size_ever_seen() {
    return jsonstats.max_size_ever_seen;
}

void jsonstats_update_max_size_ever_seen(const size_t max_size) {
    if (max_size > jsonstats.max_size_ever_seen) {
        jsonstats.max_size_ever_seen = max_size;
    }
}

unsigned long long jsonstats_get_defrag_count() {
    return jsonstats.defrag_count;
}

void jsonstats_increment_defrag_count() {
    jsonstats.defrag_count++;
}

unsigned long long jsonstats_get_defrag_bytes() {
    return jsonstats.defrag_bytes;
}

void jsonstats_increment_defrag_bytes(const size_t amount) {
    jsonstats.defrag_bytes += amount;
}

/* Given a size (bytes), find histogram bucket index using binary search.
 */
uint32_t jsonstats_find_bucket(size_t size) {
    int lo = 0;
    int hi = NUM_BUCKETS;  // length of buckets[] is NUM_BUCKETS + 1
    while (hi - lo > 1) {
        uint32_t mid = (lo + hi) / 2;
        if (size < buckets[mid])
            hi = mid;
        else if (size > buckets[mid])
            lo = mid;
        else
            return mid;
    }
    return lo;
}

/* Update the static document histogram */
STATIC void update_doc_hist(JDocument *doc, const size_t orig_size, const size_t new_size,
                            const JsonCommandType cmd_type) {
    switch (cmd_type) {
        case JSONSTATS_INSERT: {
            if (orig_size == 0) {
                uint32_t new_bucket = jsonstats_find_bucket(new_size);
                doc_hist[new_bucket]++;
                dom_set_bucket_id(doc, new_bucket);
            } else {
                update_doc_hist(doc, orig_size, new_size, JSONSTATS_UPDATE);
            }
            break;
        }
        case JSONSTATS_UPDATE: {
            if (orig_size != new_size) {
                uint32_t orig_bucket = dom_get_bucket_id(doc);
                uint32_t new_bucket = jsonstats_find_bucket(new_size);
                if (orig_bucket != new_bucket) {
                    doc_hist[orig_bucket]--;
                    doc_hist[new_bucket]++;
                    dom_set_bucket_id(doc, new_bucket);
                }
            }
            break;
        }
        case JSONSTATS_DELETE: {
            uint32_t orig_bucket = dom_get_bucket_id(doc);
            if (new_size == 0) {
                doc_hist[orig_bucket]--;
            } else {
                uint32_t new_bucket = jsonstats_find_bucket(new_size);
                if (new_bucket != orig_bucket) {
                    doc_hist[orig_bucket]--;
                    doc_hist[new_bucket]++;
                    dom_set_bucket_id(doc, new_bucket);
                }
            }
            break;
        }
        default:
            break;
    }
}

void jsonstats_sprint_hist_buckets(char *buf, const size_t buf_size) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i=0; i < NUM_BUCKETS; i++) {
        if (i > 0) oss << ",";
        oss << buckets[i];
    }
    oss << ",INF]";
    std::string str = oss.str();
    ValkeyModule_Assert(str.length() <= buf_size);
    memcpy(buf, str.c_str(), str.length());
    buf[str.length()] = '\0';
}

STATIC void sprint_hist(size_t *arr, const size_t len, char *buf, const size_t buf_size) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i=0; i < len; i++) {
        if (i > 0) oss << ",";
        oss << arr[i];
    }
    oss << "]";
    std::string str = oss.str();
    ValkeyModule_Assert(str.length() <= buf_size);
    memcpy(buf, str.c_str(), str.length());
    buf[str.length()] = '\0';
}

void jsonstats_sprint_doc_hist(char *buf, const size_t buf_size) {
    sprint_hist(doc_hist, NUM_BUCKETS, buf, buf_size);
}

void jsonstats_sprint_read_hist(char *buf, const size_t buf_size) {
    sprint_hist(read_hist, NUM_BUCKETS, buf, buf_size);
}

void jsonstats_sprint_insert_hist(char *buf, const size_t buf_size) {
    sprint_hist(insert_hist, NUM_BUCKETS, buf, buf_size);
}

void jsonstats_sprint_update_hist(char *buf, const size_t buf_size) {
    sprint_hist(update_hist, NUM_BUCKETS, buf, buf_size);
}

void jsonstats_sprint_delete_hist(char *buf, const size_t buf_size) {
    sprint_hist(delete_hist, NUM_BUCKETS, buf, buf_size);
}

void jsonstats_update_stats_on_read(const size_t fetched_val_size) {
    uint32_t bucket = jsonstats_find_bucket(fetched_val_size);
    read_hist[bucket]++;
}

void jsonstats_update_stats_on_insert(JDocument *doc, const bool is_delete_doc_key, const size_t orig_size,
                                      const size_t new_size, const size_t inserted_val_size) {
    if (is_delete_doc_key) jsonstats.num_doc_keys++;
    update_doc_hist(doc, orig_size, new_size, JSONSTATS_INSERT);
    uint32_t bucket = jsonstats_find_bucket(inserted_val_size);
    insert_hist[bucket]++;
    jsonstats_update_max_size_ever_seen(new_size);
}

void jsonstats_update_stats_on_update(JDocument *doc, const size_t orig_size, const size_t new_size,
                                      const size_t input_json_size) {
    update_doc_hist(doc, orig_size, new_size, JSONSTATS_UPDATE);
    uint32_t bucket = jsonstats_find_bucket(input_json_size);
    update_hist[bucket]++;
    jsonstats_update_max_size_ever_seen(new_size);
}

void jsonstats_update_stats_on_delete(JDocument *doc, const bool is_delete_doc_key, const size_t orig_size,
                                      const size_t new_size, const size_t deleted_val_size) {
    update_doc_hist(doc, orig_size, new_size, JSONSTATS_DELETE);
    if (is_delete_doc_key) {
        ValkeyModule_Assert(jsonstats.num_doc_keys > 0);
        jsonstats.num_doc_keys--;
    }
    uint32_t bucket = jsonstats_find_bucket(deleted_val_size);
    delete_hist[bucket]++;
}

