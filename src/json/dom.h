/**
 * DOM (Document Object Model) interface for JSON.
 * The DOM module provides the following functions:
 * 1. Parsing and validating an input JSON string buffer
 * 2. Deserializing a JSON string into document object
 * 2. Serializing a document object into JSON string
 * 3. JSON CRUD operations: search, insert, update and delete
 *
 * Design Considerations:
 * 1. Memory management: All memory management must be handled by the JSON allocator.
 *    - For memories allocated by our own code:
 *      All allocations and de-allocations must be done through:
 *      dom_alloc, dom_free, dom_realloc, dom_strdup, and dom_strndup
 *    - For objects allocated by RapidJSON library:
 *      Our solution is to use a custom memory allocator class as template, so as to instruct RapidJSON to the JSON
 *      allocator. The custom allocator works under the hood and is not exposed through this interface.
 * 2. If a method returns to the caller a heap-allocated object, it must be documented.
 *    The caller is responsible for releasing the memory after consuming it.
 * 3. Generally speaking, interface methods should not have Valkey module types such as ValkeyModuleCtx or
 *    ValkeyModuleString, because that would make unit tests hard to write, unless gmock classes have been developed.
 *
 * Coding Conventions & Best Practices:
 * 1. Error handling: If a method may fail, the return type should be enum JsonUtilCode.
 * 2. Output parameters: Output parameters should be placed at the end. Output parameters should be initialized at the
 *    beginning of the method. It should not require the caller to do any initialization before invoking the method.
 * 3. Every public interface method declared in this file should be prefixed with "dom_".
 */

#ifndef VALKEYJSONMODULE_JSON_DOM_H_
#define VALKEYJSONMODULE_JSON_DOM_H_

#include <stdlib.h>
#include <string>
#include "json/util.h"
#include "json/alloc.h"
#include "json/rapidjson_includes.h"

class ReplyBuffer : public rapidjson::StringBuffer {
 public:
    ReplyBuffer(ValkeyModuleCtx *_ctx, bool) : rapidjson::StringBuffer(), ctx(_ctx) {}
    ReplyBuffer() : rapidjson::StringBuffer(), ctx(nullptr) {}
    void Initialize(ValkeyModuleCtx *_ctx, bool) { ctx = _ctx; }
    void Reply() { ValkeyModule_ReplyWithStringBuffer(ctx, GetString(), GetLength()); }

 private:
    ValkeyModuleCtx *ctx;
};

extern "C" {
#define VALKEYMODULE_EXPERIMENTAL_API
#include <./include/valkeymodule.h>
}

/**
 * This is a custom allocator for RapidJSON. It delegates memory management to the JSON allocator, so that
 * memory allocated by the underlying RapidJSON library can be correctly reported to Valkey engine. The class
 * is passed into rapidjson::GenericDocument and rapidjson::GenericValue as template, which is the way to tell
 * RapidJSON to use a custom allocator.
 */
class RapidJsonAllocator {
 public:
    RapidJsonAllocator();

    void *Malloc(size_t size) {
        return dom_alloc(size);
    }

    void *Realloc(void *originalPtr, size_t /*originalSize*/, size_t newSize) {
        return dom_realloc(originalPtr, newSize);
    }

    static void Free(void *ptr) RAPIDJSON_NOEXCEPT {
        dom_free(ptr);
    }

    bool operator==(const RapidJsonAllocator&) const RAPIDJSON_NOEXCEPT {
        return true;
    }

    bool operator!=(const RapidJsonAllocator&) const RAPIDJSON_NOEXCEPT {
        return false;
    }

    static const bool kNeedFree = true;
};

/**
 * Now, wrap the RapidJSON objects (RJxxxxx) with our own objects (Jxxxxx). We wrap them to hide
 * various RapidJSON oddities, simplify the syntax and to more explicitly express the semantics.
 * For example, the details of allocators are largely hidden in the wrapped objects.
 *
 * We use four objects for all of our work. All of these objects descend from one of the three
 * basic RapidJSON object types.
 *
 *  RJValue (JValue):   A JSON value. This is implemented as a node of a tree. This object doesn't
 *                      differentiate between being the root of a tree or the root of a sub-tree.
 *                      The full suite of RapidJSON value manipulation functions is available. Many
 *                      of the RapidJSON value functions require an allocator. You must use the
 *                      global "allocator".
 *
 *  RJParser (JParser): This object contains a JValue into which you can deserialize a stream via the
 *                      Parse/ParseStream member functions. The JValue created by the parsing routines
 *                      is allocated using the dom_alloc/dom_free accounting. Typically, a JParser
 *                      object is created on the run-time stack, filled with some serialized data
 *                      and the then created JValue is moved into a destination location.
 *
 * JDocument            This is the only object visible external to the dom layer. Externally, a
 *                      JDocument is the Valkey data type for this module, i.e., the Valkey dictionary
 *                      contains this pointer. Externally, this is an opaque data structure.
 *                      Internally, it's implemented as a JValue plus a size and bucket number. The
 *                      size is maintained as the memory size of the entire tree of JValues
 *                      contained by the JDocument.
 */

typedef rapidjson::GenericValue<rapidjson::UTF8<>, RapidJsonAllocator> RJValue;
// A JValue is an RJValue without any local augmentation of change.
typedef RJValue JValue;

extern RapidJsonAllocator allocator;

/**
 * A JDocument privately inherits from JValue. You must use the GetJValue() member
 * to access the underlying JValue. This improves readability at the usage point.
 */
struct JDocument : JValue {
    JDocument() : JValue(), size(0), bucket_id(0) {}
    JValue& GetJValue() { return *this; }
    const JValue& GetJValue() const { return *this; }
    void SetJValue(JValue& rhs) { *static_cast<JValue *>(this) = rhs; }
    size_t size:56;        // Size of this document, maintained by the JSON layer, not here.
    size_t bucket_id:8;    // document histogram's bucket id. maintained by JSON layer, not here
    void *operator new(size_t size) { return dom_alloc(size); }
    void operator delete(void *ptr) { return dom_free(ptr); }

 private:
    //
    // Since JDocument objects are 1:1 with Valkey Keys, you can't ever have an array of them.
    //
    void *operator new[](size_t);       // Not defined anywhere, causes link error if used
    void operator delete[](void *);     // Not defined anywhere, causes link error if used
};

//
// typedef the RapidJSON objects we care about, name them RJxxxxx for clarity
//
typedef rapidjson::GenericDocument<rapidjson::UTF8<>, RapidJsonAllocator> RJParser;

/**
 * A JParser privately inherits from RJParser, which inherits from RJValue. You must use the
 * GetJValue() member to access the post Parse value.
 *
 */
struct JParser : RJParser {
    JParser() : RJParser(&allocator), allocated_size(0) {}
    //
    // Make these inner routines publicly visible
    //
    using RJParser::ParseStream;
    using RJParser::HasParseError;
    using RJParser::GetMaxDepth;
    // Access the contained JValue
    JValue& GetJValue() { return *this; }
    //
    // Translate rapidJSON parse error code into JsonUtilCode.
    //
    JsonUtilCode GetParseErrorCode() {
        switch (GetParseError()) {
            case rapidjson::kParseErrorTermination:
                return JSONUTIL_DOCUMENT_PATH_LIMIT_EXCEEDED;
            case rapidjson::kParseErrorNone:
                ValkeyModule_Assert(false);
                /* Fall Through, but not really */
            default:
                return JSONUTIL_JSON_PARSE_ERROR;
        }
    }
    //
    // When we parse an incoming string, we want to know how much member this will consume.
    // So track it and retain it.
    //
    JParser& Parse(const char *json, size_t len);
    JParser& Parse(const std::string_view &sv);
    //
    // This object holds a JValue which is the root of the parsed tree. The dom_alloc/dom_free
    // machinery will track all memory allocations outside of this object, but the root JValue
    // won't be covered by that. Because the JParser objects aren't created via new, they are
    // created as stack variables. So we manually add that in, since it'll be charged to the
    // destination when we actually move the value out.
    //
    size_t GetJValueSize() const { return allocated_size + sizeof(RJValue); }

 private:
    size_t allocated_size;
};

/* Parse input JSON string, validate syntax, and return a document object.
 * This method can handle an input string that is not NULL terminated. One use case is that
 * we call ValkeyModule_LoadStringBuffer() to load JSON data from RDB, which returns a string that
 * is not automatically NULL terminated.
 * Also note that if the input string has NULL character '\0' in the middle, the string
 * terminates at the NULL character.
 *
 * @param json_buf - pointer to binary string buffer, which may not be NULL terminated.
 * @param buf_len - length of the input string buffer, which may not be NULL terminated.
 * @param doc - OUTPUT param, pointer to document pointer. The caller is responsible for calling
 *        dom_free_doc(JDocument*) to free the memory after it's consumed.
 * @return JSONUTIL_SUCCESS for success, other code for failure.
 */
JsonUtilCode dom_parse(ValkeyModuleCtx *ctx, const char *json_buf, const size_t buf_len, JDocument **doc);

/* Free a document object */
void dom_free_doc(JDocument *doc);

/* Get document size */
size_t dom_get_doc_size(const JDocument *doc);

/* Set document size */
void dom_set_doc_size(JDocument *doc, const size_t size);

/* Get the document histogram's bucket ID */
size_t dom_get_bucket_id(const JDocument *doc);

/* Set the document histogram's bucket ID */
void dom_set_bucket_id(JDocument *doc, const uint32_t bucket_id);

/**
 * Serialize a document into the given string stream.
 * @param format - controls format of returned JSON string.
 *        if NULL, return JSON in compact format (no space, no indent, no newline).
 * @param oss - output stream
 */
void dom_serialize(JDocument *doc, const PrintFormat *format, rapidjson::StringBuffer &oss);

/**
  * Serialize a value into the given string stream.
  * @param format - controls format of returned JSON string.
  *        if NULL, return JSON in compact format (no space, no indent, no newline).
  * @param oss - output stream
  * @param json_len - OUTPUT param, *json_len is length of JSON string.
  */
void dom_serialize_value(const JValue &val, const PrintFormat *format, rapidjson::StringBuffer &oss);

/**
 * Get the root value of the document.
 */
JValue& dom_get_value(JDocument &doc);

/* Set value at the path.
 * @param json_path: path that is compliant to the JSON Path syntax.
 * @param is_create_only - indicates to create a new value.
 * @param is_update_only - indicates to update an existing value.
 * @return JSONUTIL_SUCCESS for success, other code for failure.
 */
JsonUtilCode dom_set_value(ValkeyModuleCtx *ctx, JDocument *doc, const char *json_path, const char *new_val_json,
                           size_t new_val_len, const bool is_create_only = false, const bool is_update_only = false);


inline JsonUtilCode dom_set_value(ValkeyModuleCtx *ctx, JDocument *doc, const char *json_path, const char *new_val_json,
                           const bool is_create_only = false, const bool is_update_only = false) {
    return dom_set_value(ctx, doc, json_path, new_val_json, strlen(new_val_json), is_create_only, is_update_only);
}



/* Get JSON value at the path.
 * If the path is invalid, the method will return error code JSONUTIL_INVALID_JSON_PATH.
 * If the path does not exist, the method will return error code JSONUTIL_JSON_PATH_NOT_EXIST.
 *
 * @param format - controls format of returned JSON string.
 *        if NULL, return JSON in compact format (no space, no indent, no newline).
 * @param oss - output stream
 * @return JSONUTIL_SUCCESS for success, other code for failure.
 */
template<typename T>
JsonUtilCode dom_get_value_as_str(JDocument *doc, const char *json_path, const PrintFormat *format,
                                  T &oss, const bool update_stats = true);

/* Get JSON values at multiple paths. Values at multiple paths will be aggregated into a JSON object,
 * in which each path is a key.
 * If the path is invalid, the method will return error code JSONUTIL_INVALID_JSON_PATH.
 * If the path does not exist, the method will return error code JSONUTIL_JSON_PATH_NOT_EXIST.
 *
 * @param format - controls format of returned JSON string.
 *        if NULL, return JSON in compact format (no space, no indent, no newline).
 * @param oss - output stream, the string represents an aggregated JSON object in which each path is a key.
* @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
*/
JsonUtilCode dom_get_values_as_str(JDocument *doc, const char **paths, const int num_paths,
                                   PrintFormat *format, ReplyBuffer &oss, const bool update_stats = true);

/**
 * Delete JSON values at the given path.
 * @num_vals_deleted number of values deleted
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_delete_value(JDocument *doc, const char *json_path, size_t &num_vals_deleted);

/* Increment the JSON value by a given number.
 * @param out_val OUTPUT parameter, pointer to new value
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_increment_by(JDocument *doc, const char *json_path, const JValue *incr_by,
                              jsn::vector<double> &out_vals, bool &is_v2_path);

/* Multiply the JSON value by a given number.
 * @param out_val OUTPUT parameter, pointer to new value
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_multiply_by(JDocument *doc, const char *json_path, const JValue *mult_by,
                             jsn::vector<double> &out_vals, bool &is_v2_path);

/* Toggle a JSON boolean between true and false.
 * @param vec OUTPUT parameter, a vector of integers. 0: false, 1: true, -1: N/A - the source value is not boolean.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_toggle(JDocument *doc, const char *path, jsn::vector<int> &vec, bool &is_v2_path);


/* Get the length of a JSON string value.
 * @param vec OUTPUT parameter, a vector of string lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_string_length(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path);

/* Append a string to an existing JSON string value.
 * @param vec OUTPUT parameter, a vector of new string lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_string_append(JDocument *doc, const char *path, const char *json, const size_t json_len,
                               jsn::vector<size_t> &vec, bool &is_v2_path);

/**
 * Get number of keys in the object at the given path.
 * @param vec, OUTPUT parameter, a vector of object lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_object_length(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path);

/**
 * Get keys in the object at the given path.
 * @param vec OUTPUT parameter, a vector of vector of strings. In the first level vector, number of items is
 *        number of objects. In the second level vector, number of items is number keys in the object.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_object_keys(JDocument *doc, const char *path,
                             jsn::vector<jsn::vector<jsn::string>> &vec, bool &is_v2_path);

/**
 * Get number of elements in the array at the given path.
 * @param vec OUTPUT parameter, a vector of array lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_array_length(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path);

/**
 * Append a list of values to the array at the given path.
 * @param vec OUTPUT parameter, a vector of new array lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_array_append(ValkeyModuleCtx *ctx, JDocument *doc, const char *path,
                              const char **jsons, size_t *json_lens, const size_t num_values,
                              jsn::vector<size_t> &vec, bool &is_v2_path);

/**
 * Remove and return element from the index in the array.
 * Out of range index is rounded to respective array boundaries.
 *
 * @param index - position in the array to start popping from, defaults -1 , which means the last element.
 *                Negative value means position from the last element.
 * @param vec - OUTPUT parameter, a vector of string streams, each containing JSON string of the popped element
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_array_pop(JDocument *doc, const char *path, int64_t index,
                           jsn::vector<rapidjson::StringBuffer> &vec, bool &is_v2_path);

/**
 * Insert one or more json values into the array at path before the index.
 * Inserting at index 0 prepends to the array.
 * A negative index values in interpreted as starting from the end.
 * The index must be in the array's range.
 *
 * @param vec OUTPUT parameter, a vector of new array lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_array_insert(ValkeyModuleCtx *ctx, JDocument *doc, const char *path, int64_t index,
                              const char **jsons, size_t *json_lens, const size_t num_values,
                              jsn::vector<size_t> &vec, bool &is_v2_path);

/**
 * Clear all the elements in an array or object.
 * Return number of containers cleared.
 *
 * @param elements_cleared, OUTPUT parameter, number of elements cleared
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_clear(JDocument *doc, const char *path, size_t &elements_cleared);

/*
 * Trim an array so that it becomes subarray [start, end], both inclusive.
 * If the array is empty, do nothing, return 0.
 * If start < 0, set it to 0.
 * If stop >= size, set it to size-1
 * If start >= size or start > stop, empty the array and return 0.
 *
 * @param start - start index, inclusive
 * @param stop - stop index, inclusive
 * @param vec, OUTPUT parameter, a vector of new array lengths
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_array_trim(JDocument *doc, const char *path, int64_t start, int64_t stop,
                            jsn::vector<size_t> &vec, bool &is_v2_path);


/**
 * Search for the first occurrence of a scalar JSON value in an array.
 * Out of range errors are treated by rounding the index to the array's start and end.
 * If start > stop, return -1 (not found).
 *
 * @param scalar_val - scalar value to search for
 * @param start - start index, inclusive
 * @param stop - stop index, exclusive. 0 or -1 means the last element is included.
 * @param vec OUTPUT parameter, a vector of matching indexes. -1 means value not found.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_array_index_of(JDocument *doc, const char *path, const char *scalar_val,
                                const size_t scalar_val_len, int64_t start, int64_t stop,
                                jsn::vector<int64_t> &vec, bool &is_v2_path);

/* Get type of a JSON value.
 * @param vec, OUTPUT parameter, a vector of value types.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_value_type(JDocument *doc, const char *path, jsn::vector<jsn::string> &vec, bool &is_v2_path);

/*
 * Return a JSON value in Valkey Serialization Protocol (RESP).
 * If the value is container, the response is RESP array or nested array.
 *
 * JSON null is mapped to the RESP Null Bulk String.
 * JSON boolean values are mapped to the respective RESP Simple Strings.
 * JSON integer numbers are mapped to RESP Integers.
 * JSON float or double numbers are mapped to RESP Bulk Strings.
 * JSON Strings are mapped to RESP Bulk Strings.
 * JSON Arrays are represented as RESP Arrays, where the first element is the simple string [,
 * followed by the array's elements.
 * JSON Objects are represented as RESP Arrays, where the first element is the simple string {,
 * followed by key-value pairs, each of which is a RESP bulk string.
 */
JsonUtilCode dom_reply_with_resp(ValkeyModuleCtx *ctx, JDocument *doc, const char *path);

/* Get memory size of a JSON value.
 * @param vec, OUTPUT parameter, vector of memory size.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_mem_size(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path,
                          bool default_path);

/* Get number of fields in a JSON value.
 * @param vec, OUTPUT parameter, vector of number of fields.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
JsonUtilCode dom_num_fields(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path);

/**
 * Get max path depth of a document.
 */
void dom_path_depth(JDocument *doc, size_t *depth);

/* Duplicate a JSON value. */
JDocument *dom_copy(const JDocument *source);

/*
 * The dom_save and dom_load support the ability to save and load a single JSON document
 * as a sequence of chunks of data. The advantage of chunking is that you never need a single
 * buffer that's the size of the entire serialized object. Without chunking for a large JSON
 * object you would need to reserve sufficient memory to serialize it en masse.
 *
 * dom_save synchronously serializes the object into a sequence of chunks, each chunk is
 * delivered to a callback function for disposal.
 *
 * dom_load synchronously deserializes a series of chunks of data into a JSON object.
 * It calls a callback which returns a chunk of data. That data is "owned" by dom_load until
 * it passes the ownership of that chunk back to the caller via a callback. End of input can
 * be signalled by returning a nullptr or a 0-length chunk of data.
 *
 * The usage of callbacks insulates the dom layer from any knowledge of the RDB format. That's
 * exclusively done by the callback functions.
 */

/*
 * save a document into rdb format.
 * @param source JSON document to be saved
 * @param rdb rdb file context
 */
void dom_save(const JDocument *source, ValkeyModuleIO *rdb, int encver);

/*
 * load a document from rdb format
 * @param dest output document pointer
 * @param rdb rdb file context
 * @param encver encoding version
 */
JsonUtilCode dom_load(JDocument **dest, ValkeyModuleIO *rdb, int encver);

/*
 * Implement DEBUG DIGEST
 */
void dom_compute_digest(ValkeyModuleDigest *ctx, const JDocument *doc);

void dom_dump_value(JValue &v);

// Unit test
jsn::string validate(const JDocument *);

//
// JSON Validation functions
//

//
// Validates that all pointers contained within this JValue are valid.
//
// true => All good.
// false => Not all good.
//
bool ValidateJValue(JValue &v);

//
// This function dumps a JValue to an output stream (like an ostringstream)
//
// The structure of the object is dumped but no actual customer data is dumped.
// It's totally legal to call this function on a corrupted JValue, it'll avoid the bad mallocs
//
// Typical usage:
//
//    std::ostringstream os;
//    DumpRedactedJValue(os, v);        // Don't specify level or index parameters, let them default
//    ValkeyModule_Log(...., os.str());
//
void DumpRedactedJValue(std::ostream& os, const JValue &v, size_t level = 0, int index = -1);
//
// Same as above, except targets the Valkey Log
//
void DumpRedactedJValue(const JValue &v, ValkeyModuleCtx *ctx = nullptr, const char *level = "debug");

#endif  // VALKEYJSONMODULE_JSON_DOM_H_