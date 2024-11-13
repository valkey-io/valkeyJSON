/**
 * The STATS module's main responsibility is to produce the following metrics:
 * 1. Core metrics:
 *    json_total_memory_bytes: total memory allocated to JSON objects
 *    json_num_documents: number of document keys in Valkey
 *    json_num_reads: number of reads
 *    json_num_writes: number of writes
 * 2. Histograms:
 *    json_doc_histogram: static histogram showing document size distribution. Value of the i_th element is
 *        number of documents whose size fall into bucket i.
 *    json_read_histogram: dynamic histogram for read operations (JSON.GET and JSON.MGET). Value of the i_th
 *        element is number of read operations with fetched JSON size falling into bucket i.
 *    json_insert_histogram: dynamic histogram for insert operations (JSON.SET and JSON.ARRINSERT) that either
 *        insert new documents or insert values into existing documents. Value of the i_th element is number of
 *        insert operations with inserted values' size falling into bucket i.
 *    json_update_histogram: dynamic histogram for update operations (JSON.SET, JSON.STRAPPEND and
 *        JSON.ARRAPPEND). Value of the i_th element is number of update operations with input JSON size falling into
 *        bucket i.
 *    json_delete_histogram: dynamic histogram for delete operations (JSON.DEL, JSON.FORGET, JSON.ARRPOP and
 *        JSON.ARRTRIM). Value of the i_th element is number of delete operations with deleted values' size falling
 *        into bucket i.
 *
 * Histogram buckets:
 *    [0,256), [256,1k), [1k,4k), [4k,16k), [16k,64k), [64k,256k), [256k,1m), [1m,4m), [4m,16m), [16m,64m), [64m,INF).
 *    Each bucket represents a JSON size range in bytes.
 *
 * To query metrics, run Valkey command:
 *    info modules: returns all metrics of the module
 *    info json_core_metrics: returns core metrics
 */
#ifndef VALKEYJSONMODULE_JSON_STATS_H_
#define VALKEYJSONMODULE_JSON_STATS_H_

#include "json/dom.h"

typedef enum {
    JSONSTATS_READ = 0,
    JSONSTATS_INSERT,
    JSONSTATS_UPDATE,
    JSONSTATS_DELETE
} JsonCommandType;

/* Initialize statistics counters and thread local storage (TLS) keys. */
JsonUtilCode jsonstats_init();

/* Begin tracking memory usage.
 * @return value of the thread local counter.
*/
int64_t jsonstats_begin_track_mem();

/* End tracking memory usage.
 * @param begin_val - previous saved thread local value that is returned from jsonstats_begin_track_memory().
 * @return delta of used memory
 */
int64_t jsonstats_end_track_mem(const int64_t begin_val);

/* Get the total memory allocated to JSON objects. */
unsigned long long jsonstats_get_used_mem();

/* The following two methods are invoked by the DOM memory allocator upon every malloc/free/realloc.
 * Two memory counters are updated: A global atomic counter and a thread local counter (per thread).
 */
void jsonstats_increment_used_mem(size_t delta);
void jsonstats_decrement_used_mem(size_t delta);

// get counters
unsigned long long jsonstats_get_num_doc_keys();

unsigned long long jsonstats_get_max_depth_ever_seen();
void jsonstats_update_max_depth_ever_seen(const size_t max_depth);
unsigned long long jsonstats_get_max_size_ever_seen();

unsigned long long jsonstats_get_defrag_count();
void jsonstats_increment_defrag_count();

unsigned long long jsonstats_get_defrag_bytes();
void jsonstats_increment_defrag_bytes(const size_t amount);

// updating stats on read/insert/update/delete operation
void jsonstats_update_stats_on_read(const size_t fetched_val_size);
void jsonstats_update_stats_on_insert(JDocument *doc, const bool is_delete_doc_key, const size_t orig_size,
                                      const size_t new_size, const size_t inserted_val_size);
void jsonstats_update_stats_on_update(JDocument *doc, const size_t orig_size, const size_t new_size,
                                      const size_t input_json_size);
void jsonstats_update_stats_on_delete(JDocument *doc, const bool is_delete_doc_key, const size_t orig_size,
                                      const size_t new_size, const size_t deleted_val_size);

// helper methods for printing histograms into C string
void jsonstats_sprint_hist_buckets(char *buf, const size_t buf_size);
void jsonstats_sprint_doc_hist(char *buf, const size_t buf_size);
void jsonstats_sprint_read_hist(char *buf, const size_t buf_size);
void jsonstats_sprint_insert_hist(char *buf, const size_t buf_size);
void jsonstats_sprint_update_hist(char *buf, const size_t buf_size);
void jsonstats_sprint_delete_hist(char *buf, const size_t buf_size);

/* Given a size (bytes), find the histogram bucket index using binary search.
 */
uint32_t jsonstats_find_bucket(size_t size);


/* JSON logical statistics.
 * Used for internal tracking of elements for Skyhook Billing.
 * Using a similar structure to JsonStats.
 * We don't track the logical bytes themselves here as they are tracked by Skyhook Metering.
 * We are using size_t to match Valkey Module API for Data Metering.
 */
typedef struct {
    std::atomic_size_t boolean_count;  // 16 bytes
    std::atomic_size_t number_count;  // 16 bytes
    std::atomic_size_t sum_extra_numeric_chars;  // 1 byte per char
    std::atomic_size_t string_count;  // 16 bytes
    std::atomic_size_t sum_string_chars;  // 1 byte per char
    std::atomic_size_t null_count;  // 16 bytes
    std::atomic_size_t array_count;  // 16 bytes
    std::atomic_size_t sum_array_elements;  // internal metric
    std::atomic_size_t object_count;  // 16 bytes
    std::atomic_size_t sum_object_members;  // internal metric
    std::atomic_size_t sum_object_key_chars;  // 1 byte per char

    void reset() {
        boolean_count = 0;
        number_count = 0;
        sum_extra_numeric_chars = 0;
        string_count = 0;
        sum_string_chars = 0;
        null_count = 0;
        array_count = 0;
        sum_array_elements = 0;
        object_count = 0;
        sum_object_members = 0;
        sum_object_key_chars = 0;
    }
} LogicalStats;
extern LogicalStats logical_stats;

#define DOUBLE_CHARS_CUTOFF 24

#endif  // VALKEYJSONMODULE_JSON_STATS_H_
