/**
 * This file implements the Valkey Module interfaces.
 *
 * When the module is loaded, it does the following:
 * 1. register the JSON module.
 * 2. register callback methods such as rdb_load, rdb_save, free, etc.
 * 3. register JSON data type.
 * 4. register commands that are all prefixed with "JSON.".
 *
 * Design Considerations:
 * 1. Command API: see API.md.
 * 2. All JSON CRUD operations should be delegated to the DOM module.
 * 3. Shared utility/helper code should reside in the UTIL module.
 * 4. When invoking a DOM or UTIL method tha returns a heap-allocated object, the caller must release the memory
 *    after consuming it.
 * 5. The first line of every command handler should be: "ValkeyModule_AutoMemory(ctx);". This is for enabling
 *    auto memory management for the command.
 * 6. Every write command must support replication. Call "ValkeyModule_ReplicateVerbatim(ctx)" to tell Valkey to
 *    replicate the command.
 * 7. Any write command that increases total memory utilization, should be created using "write deny-oom" flags.
 *    e.g., JSON.SET should be defined as "write deny-oom", while JSON.DEL does not need "deny-oom" as it can't
 *    increase the total memory.
 *
 * Coding Conventions & Best Practices:
 * 1. Every command handler is named as Command_JsonXXX, where XXX is command name.
 * 2. Every callback method is named as DocumentType_XXX, where XXX indicates callback interface method.
 * 3. Majority of the code are command handler methods. Command arguments processing code are separated out into
 *    helper structs named as XXXCmdsArgs, and helper methods named as parseXXXCmdArgs, where XXX is command name.
 */

#include "json/json.h"
#include "json/dom.h"
#include "json/rapidjson_includes.h"
#include "json/alloc.h"
#include "json/stats.h"
#include "json/memory.h"
#include "./include/valkeymodule.h"
#include <string>
#include <memory>
#include <cmath>

#define MODULE_VERSION 10201
#define MODULE_NAME "json"
#define DOCUMENT_TYPE_NAME "ReJSON-RL"
#define DOCUMENT_TYPE_ENCODING_VERSION 3   /* Currently support 1 or 3 */

#define ERRMSG_JSON_DOCUMENT_NOT_FOUND "NONEXISTENT JSON document is not found"
#define ERRMSG_NEW_VALKEY_KEY_PATH_NOT_ROOT "SYNTAXERR A new Valkey key's path must be root"
#define ERRMSG_CANNOT_DISABLE_MODULE_DUE_TO_OUTSTADING_DATA \
    "Cannot disable the module because there are outstanding document keys"

#define STATIC /* decorator for static functions, remove so that backtrace symbols include these */

ValkeyModuleType *DocumentType;  // Module type

#define DEFAULT_MAX_DOCUMENT_SIZE (0) // Infinite
#define DEFAULT_DEFRAG_THRESHOLD (64 * 1024 * 1024)  // 64MB
static size_t config_max_document_size = DEFAULT_MAX_DOCUMENT_SIZE;
static size_t config_defrag_threshold = DEFAULT_DEFRAG_THRESHOLD;

#define DEFAULT_MAX_PATH_LIMIT 128
static size_t config_max_path_limit = DEFAULT_MAX_PATH_LIMIT;

#define DEFAULT_MAX_PARSER_RECURSION_DEPTH 200
static size_t config_max_parser_recursion_depth = DEFAULT_MAX_PARSER_RECURSION_DEPTH;

#define DEFAULT_MAX_RECURSIVE_DESCENT_TOKENS 20
static size_t config_max_recursive_descent_tokens = DEFAULT_MAX_RECURSIVE_DESCENT_TOKENS;

#define DEFAULT_MAX_QUERY_STRING_SIZE (128 * 1024)  // 128KB
static size_t config_max_query_string_size = DEFAULT_MAX_QUERY_STRING_SIZE;

KeyTable *keyTable = nullptr;
rapidjson::HashTableFactors rapidjson::hashTableFactors;
rapidjson::HashTableStats   rapidjson::hashTableStats;

bool enforce_rdb_version_check = false;

extern size_t hash_function(const char *text, size_t length);

size_t json_get_max_document_size() {
    return config_max_document_size;
}

size_t json_get_defrag_threshold() {
    return config_defrag_threshold;
}

size_t json_get_max_path_limit() {
    return config_max_path_limit;
}

size_t json_get_max_parser_recursion_depth() {
    return config_max_parser_recursion_depth;
}

size_t json_get_max_recursive_descent_tokens() {
    return config_max_recursive_descent_tokens;
}

size_t json_get_max_query_string_size() {
    return config_max_query_string_size;
}

#define CHECK_DOCUMENT_SIZE_LIMIT(ctx, new_doc_size) \
if (!(ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_REPLICATED) && \
    json_get_max_document_size() > 0 && (new_doc_size > json_get_max_document_size())) { \
    return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(JSONUTIL_DOCUMENT_SIZE_LIMIT_EXCEEDED)); \
}

// module config params
// NOTE: We save a copy of the value for each config param instead of pointer address, because the compiler does
// not allow casting const pointer to pointer.

extern "C" {
    #define VALKEYMODULE_EXPERIMENTAL_API
}

// instrumentation configs
static int instrument_enabled_insert = 0;
static int instrument_enabled_update = 0;
static int instrument_enabled_delete = 0;
static int instrument_enabled_dump_doc_before = 0;
static int instrument_enabled_dump_doc_after = 0;
static int instrument_enabled_dump_value_before_delete = 0;

bool json_is_instrument_enabled_insert() {
    return instrument_enabled_insert == 1;
}
bool json_is_instrument_enabled_update() {
    return instrument_enabled_update == 1;
}
bool json_is_instrument_enabled_delete() {
    return instrument_enabled_delete == 1;
}
bool json_is_instrument_enabled_dump_doc_before() {
    return instrument_enabled_dump_doc_before == 1;
}
bool json_is_instrument_enabled_dump_doc_after() {
    return instrument_enabled_dump_doc_after == 1;
}
bool json_is_instrument_enabled_dump_value_before_delete() {
    return instrument_enabled_dump_value_before_delete == 1;
}

#define REGISTER_BOOL_CONFIG(ctx, name, default_val, privdata, getfn, setfn) { \
    if (ValkeyModule_RegisterBoolConfig(ctx, name, default_val, VALKEYMODULE_CONFIG_DEFAULT, \
        getfn, setfn, nullptr, privdata) == VALKEYMODULE_ERR) { \
        ValkeyModule_Log(ctx, "warning", "Failed to register module config \"%s\".", name); \
        return VALKEYMODULE_ERR; \
    } \
}

#define REGISTER_NUMERIC_CONFIG(ctx, name, default_val, flag, min, max, privdata, getfn, setfn) { \
    if (ValkeyModule_RegisterNumericConfig(ctx, name, default_val, flag, min, max, \
        getfn, setfn, nullptr, privdata) == VALKEYMODULE_ERR ) { \
        ValkeyModule_Log(ctx, "warning", "Failed to register module config \"%s\".", name); \
        return VALKEYMODULE_ERR; \
    } \
}

/* ============================== Helper Methods ============================== */

/* Verify that the document key exists and is a document key.
 * @param key - OUTPUT parameter, pointer to ValkeyModuleKey pointer.
 */
STATIC JsonUtilCode verify_doc_key(ValkeyModuleCtx *ctx, ValkeyModuleString *rmKey, ValkeyModuleKey **key,
                                                                                     bool readOnly = false) {
    *key = static_cast<ValkeyModuleKey*>(ValkeyModule_OpenKey(ctx, rmKey,
                                        readOnly?  VALKEYMODULE_READ : VALKEYMODULE_READ | VALKEYMODULE_WRITE));
    if (ValkeyModule_KeyType(*key) == VALKEYMODULE_KEYTYPE_EMPTY) return JSONUTIL_DOCUMENT_KEY_NOT_FOUND;
    if (ValkeyModule_ModuleTypeGetType(*key) != DocumentType) return JSONUTIL_NOT_A_DOCUMENT_KEY;
    return JSONUTIL_SUCCESS;
}

/* Fetch JSON at a single path.
 * If the document key does not exist, the command will return null without an error.
 * If the key is not a document key, the command will return error code JSONUTIL_NOT_A_DOCUMENT_KEY.
 * If the JSON path is invalid or does not exist, the method will return error code JSONUTIL_INVALID_JSON_PATH.
 *
 * @param format - controls format of returned JSON string.
 *        if nullptr, return JSON in compact format (no space, no indent, no newline).
 * @param oss - output stream
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
STATIC JsonUtilCode fetch_json(ValkeyModuleCtx *ctx, ValkeyModuleString *rmKey, const char *path,
                               PrintFormat *format, ReplyBuffer &oss) {
    ValkeyModuleKey *key;
    JsonUtilCode rc = verify_doc_key(ctx, rmKey, &key, true);
    if (rc != JSONUTIL_SUCCESS) return rc;

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // fetch value at the path
    return dom_get_value_as_str(doc, path, format, oss);
}

/* Fetch JSON at multiple paths. Values at multiple paths will be aggregated into a JSON object,
 * in which each path is a key.
 * If the document key does not exist, the command will return null without an error.
 * If the key is not a document key, the command will return error code JSONUTIL_NOT_A_DOCUMENT_KEY.
 * If the JSON path is invalid or does not exist, the path's corresponding value will be JSON null.
 *
 * @param format - controls format of returned JSON string.
 *        if nullptr, return JSON in compact format (no space, no indent, no newline).
 * @param oss - output stream, the string represents an aggregated JSON object in which each path is a key.
 * @return JSONUTIL_SUCCESS if success. Other codes indicate failure.
 */
STATIC JsonUtilCode fetch_json_multi_paths(ValkeyModuleCtx *ctx, ValkeyModuleString *rmKey, const char **paths,
                                           const int num_paths, PrintFormat *format, ReplyBuffer &oss) {
    ValkeyModuleKey *key;
    JsonUtilCode rc = verify_doc_key(ctx, rmKey, &key, true);
    if (rc != JSONUTIL_SUCCESS) return rc;

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // fetch values at the path
    return dom_get_values_as_str(doc, paths, num_paths, format, oss);
}

/* ================= Helper Methods: Parsing Command Args ================== */

typedef struct {
    ValkeyModuleString *key;  // required
    const char *path;        // required
    const char *json;        // required
    size_t json_len;

    // The following two booleans map to the optional arg "NX | XX".
    // NX - set the key only if it does not exist. XX - set the key only if it exists.
    bool is_create_only;  // NX: set the key only if it does not exist
    bool is_update_only;  // XX: set the key only if it exists.
} SetCmdArgs;

STATIC JsonUtilCode parseSetCmdArgs(ValkeyModuleString **argv, const int argc, SetCmdArgs *args) {
    memset(args, 0, sizeof(SetCmdArgs));

    // we need 4 or 5 arguments
    if (argc != 4 && argc != 5) {
        return JSONUTIL_WRONG_NUM_ARGS;
    }

    args->key = argv[1];
    args->path = ValkeyModule_StringPtrLen(argv[2], nullptr);
    args->json = ValkeyModule_StringPtrLen(argv[3], &args->json_len);

    if (argc == 5) {
        const char *cond = ValkeyModule_StringPtrLen(argv[4], nullptr);
        if (!strcasecmp(cond, "NX")) {
            args->is_create_only = true;
        } else if (!strcasecmp(cond, "XX")) {
            args->is_update_only = true;
        } else {
            return JSONUTIL_COMMAND_SYNTAX_ERROR;
        }
    }
    return JSONUTIL_SUCCESS;
}

STATIC JsonUtilCode parseGetCmdArgs(ValkeyModuleString **argv, const int argc, ValkeyModuleString **key,
                                    PrintFormat *format, ValkeyModuleString ***paths, int *num_paths) {
    *key = nullptr;
    memset(format, 0, sizeof(PrintFormat));
    *paths = nullptr;
    *num_paths = 0;

    // we need at least 2 arguments
    if (argc < 2) return JSONUTIL_WRONG_NUM_ARGS;

    *key = argv[1];
    int i = 2;  // index of the next arg to process
    int path_count = 0;
    ValkeyModuleString **first_path = nullptr;

    // Process the remaining arguments and verify that all path arguments are positioned at the end.
    // If an arg is not one of 4 options (NEWLINE/SPACE/INDENT/NOESCAPE), treat it as path argument,
    // increment the path count, and continue. Whenever one of the 4 options is found, check the path count.
    // If it is > 0, which means there is at least one path argument in the middle (not at the end), then
    // exit the loop and return an error code.
    //
    // If the argument is one of NEWLINE/SPACE/INDENT but it is the last argument, return with error, because
    // the argument requires a following argument.
    while (i < argc) {
        const char *token = ValkeyModule_StringPtrLen(argv[i], nullptr);
        if (!strcasecmp(token, "NEWLINE")) {
            if (i == argc - 1) return JSONUTIL_COMMAND_SYNTAX_ERROR;
            format->newline = ValkeyModule_StringPtrLen(argv[++i], nullptr);
        } else if (!strcasecmp(token, "SPACE")) {
            if (i == argc - 1) return JSONUTIL_COMMAND_SYNTAX_ERROR;
            format->space = ValkeyModule_StringPtrLen(argv[++i], nullptr);
        } else if (!strcasecmp(token, "INDENT")) {
            if (i == argc - 1) return JSONUTIL_COMMAND_SYNTAX_ERROR;
            format->indent = ValkeyModule_StringPtrLen(argv[++i], nullptr);
        } else if (!strcasecmp(token, "NOESCAPE")) {
            // NOESCAPE is only for legacy compatibility and is noop.
        } else {
            // treat it as a path argument
            path_count++;
            if (first_path == nullptr) first_path = &argv[i];
        }
        ++i;
    }

    *paths = first_path;
    *num_paths = path_count;
    return JSONUTIL_SUCCESS;
}

/* A helper method to parse a simple command, which has two arguments:
 * key: required
 * path: optional, defaults to root path
 */
STATIC JsonUtilCode parseSimpleCmdArgs(ValkeyModuleString **argv, const int argc,
                                       ValkeyModuleString **key, const char **path) {
    *key = nullptr;
    *path = nullptr;

    // there should be either 2 or 3 arguments
    if (argc != 2 && argc != 3) return JSONUTIL_WRONG_NUM_ARGS;

    *key = argv[1];
    if (argc == 3) {
        *path = ValkeyModule_StringPtrLen(argv[2], nullptr);
    }
    if (*path == nullptr) *path = ".";  // default to root path
    return JSONUTIL_SUCCESS;
}

STATIC JsonUtilCode parseNumIncrOrMultByCmdArgs(ValkeyModuleString **argv, const int argc,
                                                ValkeyModuleString **key, const char **path, JValue *jvalue) {
    *key = nullptr;
    *path = nullptr;

    // we need exactly 4 arguments
    if (argc != 4) return JSONUTIL_WRONG_NUM_ARGS;

    *key = argv[1];
    *path = ValkeyModule_StringPtrLen(argv[2], nullptr);

    JParser parser;
    size_t arg_length;
    const char *arg = ValkeyModule_StringPtrLen(argv[3], &arg_length);
    if (parser.Parse(arg, arg_length).HasParseError() || !parser.GetJValue().IsNumber()) {
        return JSONUTIL_VALUE_NOT_NUMBER;
    }
    *jvalue = parser.GetJValue();
    return JSONUTIL_SUCCESS;
}

STATIC JsonUtilCode parseStrAppendCmdArgs(ValkeyModuleString **argv, const int argc,
                                          ValkeyModuleString **key, const char **path,
                                          const char**json, size_t *json_len) {
    *key = nullptr;
    *path = ".";  // defaults to root path
    *json = nullptr;
    *json_len = 0;

    // we need exactly 3 or 4 arguments
    if (argc != 3 && argc != 4) return JSONUTIL_WRONG_NUM_ARGS;

    *key = argv[1];
    if (argc == 3) {
        *json = ValkeyModule_StringPtrLen(argv[2], json_len);
    } else {
        *path = ValkeyModule_StringPtrLen(argv[2], nullptr);
        *json = ValkeyModule_StringPtrLen(argv[3], json_len);
    }
    return JSONUTIL_SUCCESS;
}

typedef struct {
    ValkeyModuleString *key;   // required
    const char *path;         // required
    long num_values;          // number of values to append
    const char **jsons;
    size_t *json_len_arr;
    size_t total_json_len;
} ArrAppendCmdArgs;

STATIC JsonUtilCode parseArrAppendCmdArgs(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, const int argc,
                                          ArrAppendCmdArgs *args) {
    memset(args, 0, sizeof(ArrAppendCmdArgs));

    // we need at least 4 arguments
    if (argc < 4) return JSONUTIL_WRONG_NUM_ARGS;

    args->key = argv[1];
    args->path = ValkeyModule_StringPtrLen(argv[2], nullptr);
    args->num_values = argc - 3;
    args->jsons = static_cast<const char**>(ValkeyModule_PoolAlloc(ctx, args->num_values * sizeof(const char *)));
    args->json_len_arr = static_cast<size_t*>(ValkeyModule_PoolAlloc(ctx, args->num_values * sizeof(size_t)));
    for (int i=0; i < args->num_values; i++) {
        args->jsons[i] = ValkeyModule_StringPtrLen(argv[i+3], &(args->json_len_arr[i]));
        args->total_json_len += args->json_len_arr[i];
    }

    return JSONUTIL_SUCCESS;
}

STATIC JsonUtilCode parseArrPopCmdArgs(ValkeyModuleString **argv, const int argc,
                                       ValkeyModuleString **key, const char **path, int64_t *index) {
    *key = nullptr;
    *path = ".";  // defaults to the root path if not provided
    *index = -1;  // defaults to -1 if not provided, which means the last element.

    // there should be 2 or 3 or 4 arguments
    if (argc != 2 && argc != 3 && argc != 4) return JSONUTIL_WRONG_NUM_ARGS;

    *key = argv[1];
    if (argc > 2) *path = ValkeyModule_StringPtrLen(argv[2], nullptr);
    if (argc > 3) {
        long long idx = 0;
        if (ValkeyModule_StringToLongLong(argv[3], &idx) == VALKEYMODULE_ERR) return JSONUTIL_VALUE_NOT_INTEGER;
        *index = idx;
    }
    return JSONUTIL_SUCCESS;
}

typedef struct {
    ValkeyModuleString *key;  // required
    const char *path;        // required
    int64_t index;           // required
    long num_values;         // number of values to insert
    const char **jsons;
    size_t *json_len_arr;
    size_t total_json_len;
} ArrInsertCmdArgs;

STATIC JsonUtilCode parseArrInsertCmdArgs(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, const int argc,
                                          ArrInsertCmdArgs *args) {
    memset(args, 0, sizeof(ArrInsertCmdArgs));

    // we need at least 5 arguments
    if (argc < 5) return JSONUTIL_WRONG_NUM_ARGS;

    args->key = argv[1];
    args->path = ValkeyModule_StringPtrLen(argv[2], nullptr);

    long long index = 0;
    if (ValkeyModule_StringToLongLong(argv[3], &index) == VALKEYMODULE_ERR) return JSONUTIL_VALUE_NOT_INTEGER;
    args->index = index;

    args->num_values = argc - 4;
    args->jsons = static_cast<const char**>(ValkeyModule_PoolAlloc(ctx, args->num_values * sizeof(const char *)));
    args->json_len_arr = static_cast<size_t*>(ValkeyModule_PoolAlloc(ctx, args->num_values * sizeof(size_t)));
    for (int i=0; i < args->num_values; i++) {
        args->jsons[i] = ValkeyModule_StringPtrLen(argv[i+4], &(args->json_len_arr[i]));
        args->total_json_len += args->json_len_arr[i];
    }
    return JSONUTIL_SUCCESS;
}

/*
 * A helper method to parse arguments for ArrayTrim command.
 * @param start - start index, inclusive
 * @param stop - stop index, inclusive
 * @return
 */
STATIC JsonUtilCode parseArrTrimCmdArgs(ValkeyModuleString **argv, const int argc,
                                        ValkeyModuleString **key, const char **path, int64_t *start, int64_t *stop) {
    *key = nullptr;
    *path = nullptr;
    *start = 0;
    *stop = 0;

    // we need exactly 5 arguments
    if (argc != 5) return JSONUTIL_WRONG_NUM_ARGS;

    *key = argv[1];
    *path = ValkeyModule_StringPtrLen(argv[2], nullptr);

    long long start_idx = 0;
    if (ValkeyModule_StringToLongLong(argv[3], &start_idx) == VALKEYMODULE_ERR) return JSONUTIL_VALUE_NOT_INTEGER;
    *start = start_idx;

    long long stop_idx = 0;
    if (ValkeyModule_StringToLongLong(argv[4], &stop_idx) == VALKEYMODULE_ERR) return JSONUTIL_VALUE_NOT_INTEGER;
    *stop = stop_idx;
    return JSONUTIL_SUCCESS;
}

typedef struct {
    ValkeyModuleString *key;  // required
    const char *path;        // required
    const char *scalar_val;  // required, scalar json value
    size_t scalar_val_len;
    int64_t start;           // optional, start index, inclusive, defaults to 0
    int64_t stop;            // optional, stop index, exclusive, defaults to 0
} ArrIndexCmdArgs;

STATIC JsonUtilCode parseArrIndexCmdArgs(ValkeyModuleString **argv, const int argc, ArrIndexCmdArgs *args) {
    memset(args, 0, sizeof(ArrIndexCmdArgs));

    // there should be 4 or 5 or 6 arguments
    if (argc != 4 && argc != 5 && argc != 6) return JSONUTIL_WRONG_NUM_ARGS;

    args->key = argv[1];
    args->path = ValkeyModule_StringPtrLen(argv[2], nullptr);
    args->scalar_val = ValkeyModule_StringPtrLen(argv[3], &args->scalar_val_len);

    if (argc > 4) {
        long long start = 0;
        if (ValkeyModule_StringToLongLong(argv[4], &start) == VALKEYMODULE_ERR) return JSONUTIL_VALUE_NOT_INTEGER;
        args->start = start;
    }

    if (argc > 5) {
        long long stop = 0;
        if (ValkeyModule_StringToLongLong(argv[5], &stop) == VALKEYMODULE_ERR) return JSONUTIL_VALUE_NOT_INTEGER;
        args->stop = stop;
    }
    return JSONUTIL_SUCCESS;
}

STATIC JsonUtilCode parseMemoryOrFieldsSubCmdArgs(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, const int argc,
                                                  ValkeyModuleKey **key, const char **path, bool& default_path) {
    *key = nullptr;
    *path = ".";  // defaults to the root path
    default_path = true;

    // there should be either 3 or 4 arguments
    if (argc != 3 && argc != 4) return JSONUTIL_WRONG_NUM_ARGS;

    JsonUtilCode rc = verify_doc_key(ctx, argv[2], key, true);
    if (rc != JSONUTIL_SUCCESS) return rc;

    if (argc > 3) {
        *path = ValkeyModule_StringPtrLen(argv[3], nullptr);
        default_path = false;
    }
    return JSONUTIL_SUCCESS;
}

/* ============================= Command Handlers =========================== */

int Command_JsonSet(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    SetCmdArgs args;
    JsonUtilCode rc = parseSetCmdArgs(argv, argc, &args);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS)
            return ValkeyModule_WrongArity(ctx);
        else
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // verify valkey keys
    ValkeyModuleKey *key = static_cast<ValkeyModuleKey*>(ValkeyModule_OpenKey(ctx, args.key,
                                                                           VALKEYMODULE_READ | VALKEYMODULE_WRITE));
    int type = ValkeyModule_KeyType(key);
    if (type != VALKEYMODULE_KEYTYPE_EMPTY && ValkeyModule_ModuleTypeGetType(key) != DocumentType) {
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(JSONUTIL_NOT_A_DOCUMENT_KEY));
    }

    bool is_new_valkey_key = (type == VALKEYMODULE_KEYTYPE_EMPTY);
    bool is_root_path = jsonutil_is_root_path(args.path);

    if (is_new_valkey_key) {
        if (!is_root_path)
            return ValkeyModule_ReplyWithError(ctx, ERRMSG_NEW_VALKEY_KEY_PATH_NOT_ROOT);
        if (args.is_update_only)
            return ValkeyModule_ReplyWithNull(ctx);
    } else {
        if (is_root_path && args.is_create_only)
            return ValkeyModule_ReplyWithNull(ctx);
    }

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    if (is_root_path) {  // root doc
        // parse incoming JSON string
        JDocument *doc;
        rc = dom_parse(ctx, args.json, args.json_len, &doc);
        if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

        // end tracking memory
        int64_t delta = jsonstats_end_track_mem(begin_val);
        size_t doc_size = dom_get_doc_size(doc) + delta;
        dom_set_doc_size(doc, doc_size);

        if (json_is_instrument_enabled_insert() || json_is_instrument_enabled_update()) {
            size_t len;
            const char* key_cstr = ValkeyModule_StringPtrLen(args.key, &len);
            std::size_t key_hash = std::hash<std::string_view>{}(std::string_view(key_cstr, len));
            ValkeyModule_Log(ctx, "warning",
                            "Dump document structure before setting JSON key (hashed) %zu whole doc %p:",
                            key_hash, static_cast<void *>(doc));
            DumpRedactedJValue(doc->GetJValue(), nullptr, "warning");
        }

        // set Valkey key
        ValkeyModule_ModuleTypeSetValue(key, DocumentType, doc);

        // update stats
        jsonstats_update_stats_on_insert(doc, true, 0, doc_size, doc_size);
    } else {
        // fetch doc object from Valkey dict
        JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
        if (doc == nullptr) return ValkeyModule_ReplyWithError(ctx, ERRMSG_JSON_DOCUMENT_NOT_FOUND);

        size_t orig_doc_size = dom_get_doc_size(doc);
        rc = dom_set_value(ctx, doc, args.path, args.json, args.json_len, args.is_create_only, args.is_update_only);
        if (rc != JSONUTIL_SUCCESS) {
            if (rc == JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED)
                return ValkeyModule_ReplyWithNull(ctx);
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }

        // end tracking memory
        int64_t delta = jsonstats_end_track_mem(begin_val);
        size_t new_doc_size = dom_get_doc_size(doc) + delta;
        dom_set_doc_size(doc, new_doc_size);

        // update stats
        jsonstats_update_stats_on_update(doc, orig_doc_size, new_doc_size, args.json_len);
    }

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);
    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.set", args.key);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int Command_JsonGet(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    PrintFormat format;
    ValkeyModuleString **paths;
    int num_paths;
    JsonUtilCode rc = parseGetCmdArgs(argv, argc, &key_str, &format, &paths, &num_paths);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS)
            return ValkeyModule_WrongArity(ctx);
        else
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch json
    ReplyBuffer oss(ctx, true);
    if (num_paths == 0) {
        // default to the root path
        rc = fetch_json(ctx, key_str, ".", &format, oss);
    } else if (num_paths == 1) {
        const char *cstr_path = ValkeyModule_StringPtrLen(paths[0], nullptr);
        rc = fetch_json(ctx, key_str, cstr_path, &format, oss);
    } else {
        const char **cstr_paths = static_cast<const char **>(ValkeyModule_PoolAlloc(ctx,
                                                                                   num_paths * sizeof(const char*)));
        int format_args_offset = 0;
        for (int i = 0; i < num_paths; i++) {
            const char *token = ValkeyModule_StringPtrLen(paths[i+format_args_offset], nullptr);
            // no need to check on the first one, we already know it's pointing to the right place
            bool look_for_formatting = i > 0;

            // we already know from parseGetCmdArgs that we're going to find another path eventually
            while (look_for_formatting) {
                look_for_formatting = false;
                if (!strcasecmp(token, "NEWLINE") || !strcasecmp(token, "SPACE") || !strcasecmp(token, "INDENT")) {
                    format_args_offset += 2;
                    look_for_formatting = true;
                } else if (!strcasecmp(token, "NOESCAPE")) {
                    format_args_offset++;
                    look_for_formatting = true;
                }
                if (look_for_formatting) {
                    token = ValkeyModule_StringPtrLen(paths[i+format_args_offset], nullptr);
                }
            }
            cstr_paths[i] = ValkeyModule_StringPtrLen(paths[i+format_args_offset], nullptr);
        }
        rc = fetch_json_multi_paths(ctx, key_str, cstr_paths, num_paths, &format, oss);
    }

    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND)
            return ValkeyModule_ReplyWithNull(ctx);
        else
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // return JSON to client
    oss.Reply();
    return VALKEYMODULE_OK;
}

int Command_JsonMGet(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    // we need at least 3 arguments
    if (argc < 3) return ValkeyModule_WrongArity(ctx);

    int num_keys = argc - 2;
    const char *path = ValkeyModule_StringPtrLen(argv[argc-1], nullptr);

    // create a vector of string streams to store JSON per key
    jsn::vector<ReplyBuffer> vec(num_keys);
    for (int i=0; i < num_keys; i++) {
        vec[i].Initialize(ctx, false);
        JsonUtilCode rc = fetch_json(ctx, argv[i + 1], path, nullptr, vec[i]);
        if (rc != JSONUTIL_SUCCESS && rc != JSONUTIL_DOCUMENT_KEY_NOT_FOUND &&
            rc != JSONUTIL_INVALID_JSON_PATH && rc != JSONUTIL_JSON_PATH_NOT_EXIST) {
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }
    }

    // return array of bulk strings to client
    ValkeyModule_ReplyWithArray(ctx, num_keys);
    for (int i=0; i < num_keys; i++) {
        if (vec[i].GetLength() == 0) {
            ValkeyModule_ReplyWithNull(ctx);
        } else {
            vec[i].Reply();
        }
    }
    return VALKEYMODULE_OK;
}

int Command_JsonDel(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS)
            return ValkeyModule_WrongArity(ctx);
        else
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) {
            // ignore non-existing keys
            return ValkeyModule_ReplyWithLongLong(ctx, 0);
        } else {
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }
    }

    if (jsonutil_is_root_path(path)) {
        if (json_is_instrument_enabled_delete()) {
            size_t len;
            const char* key_cstr = ValkeyModule_StringPtrLen(key_str, &len);
            std::size_t key_hash = std::hash<std::string_view>{}(std::string_view(key_cstr, len));
            ValkeyModule_Log(ctx, "warning", "deleting whole JSON key (hashed) %zu", key_hash);
        }

        // delete the key from Valkey Dict
        ValkeyModule_DeleteKey(key);
        // replicate the command
        ValkeyModule_ReplicateVerbatim(ctx);

        ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.del", key_str);

        return ValkeyModule_ReplyWithLongLong(ctx, 1);
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    // delete value at path
    size_t num_vals_deleted;
    rc = dom_delete_value(doc, path, num_vals_deleted);

    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_INVALID_JSON_PATH || rc == JSONUTIL_JSON_PATH_NOT_EXIST) {
            // ignore invalid or non-existent path
            return ValkeyModule_ReplyWithLongLong(ctx, 0);
        } else {
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }
    }

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_delete(doc, false, orig_doc_size, new_doc_size, abs(delta));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.del", key_str);
    return ValkeyModule_ReplyWithLongLong(ctx, num_vals_deleted);
}

/**
 * A helper method to send a reply to the client for JSON.NUMINCRBY or JSON.NUMMULTBY.
 */
STATIC void reply_numincrby_nummultby(jsn::vector<double> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the last updated number value.
        for (auto it = vec.rbegin(); it != vec.rend(); it++) {
            if (!std::isnan(*it)) {  // NaN indicates wrong object type
                    char buf[BUF_SIZE_DOUBLE_JSON];
                    size_t len = jsonutil_double_to_string(*it, buf, sizeof(buf));
                    ValkeyModule_ReplyWithStringBuffer(ctx, buf, len);
                    return;
            }
        }
        // It's impossible to reach here, because the upstream method has verified there is at least one number value.
        ValkeyModule_Assert(false);
    } else {
        // JSONPath: return serialized string of an array of values.
        // If a value is NaN, its corresponding returned element is JSON null.
        jsn::string s = "[";
        for (uint i=0; i < vec.size(); i++) {
            if (i > 0) s.append(",");
            if (std::isnan(vec[i])) {
                s.append("null");
            } else {
                char double_to_string_buf[BUF_SIZE_DOUBLE_JSON];
                jsonutil_double_to_string(vec[i], double_to_string_buf, sizeof(double_to_string_buf));
                s.append(double_to_string_buf);
            }
        }
        s.append("]");
        ValkeyModule_ReplyWithStringBuffer(ctx, s.c_str(), s.length());
    }
}

int Command_JsonNumIncrBy(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JValue jvalue;
    JsonUtilCode rc = parseNumIncrOrMultByCmdArgs(argv, argc, &key_str, &path, &jvalue);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // increment the value at path
    jsn::vector<double> vec;
    bool is_v2_path;
    rc = dom_increment_by(doc, path, &jvalue, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.numincrby", key_str);

    // convert the result to bulk string and send the reply to the client
    reply_numincrby_nummultby(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

int Command_JsonNumMultBy(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JValue jvalue;
    JsonUtilCode rc = parseNumIncrOrMultByCmdArgs(argv, argc, &key_str, &path, &jvalue);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // multiply the value at path
    jsn::vector<double> vec;
    bool is_v2_path;
    rc = dom_multiply_by(doc, path, &jvalue, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.nummultby", key_str);

    // convert the result to bulk string and send the reply to the client
    reply_numincrby_nummultby(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for JSON.STRLEN and JSON.OBJLEN.
 */
STATIC void reply_strlen_objlen(jsn::vector<size_t> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (*it != SIZE_MAX) {  // SIZE_MAX indicates wrong object type
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
                return;
            }
        }
        // It's impossible to reach here, because the upstream method has verified there is at least
        // one string/object value.
        ValkeyModule_Assert(false);
    } else {
        // JSONPath: return an array of lengths.
        // If a value is SIZE_MAX, its corresponding element is null.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if ((*it) == SIZE_MAX) {  // SIZE_MAX indicates wrong object type
                ValkeyModule_ReplyWithNull(ctx);
            } else {
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
            }
        }
    }
}

int Command_JsonStrLen(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get string lengths
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_string_length(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    reply_strlen_objlen(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for JSON.STRAPPEND.
 */
STATIC void reply_strappend(jsn::vector<size_t> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the last updated string's length.
        for (auto it = vec.rbegin(); it != vec.rend(); it++) {
            if (*it != SIZE_MAX) {  // SIZE_MAX indicates wrong object type
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
                return;
            }
        }
        // It's impossible to reach here, because the upstream method has verified there is at least
        // one string value.
        ValkeyModule_Assert(false);
    } else {
        // JSONPath: return an array of lengths.
        // If a value is SIZE_MAX, its corresponding element is null.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if ((*it) == SIZE_MAX) {  // SIZE_MAX indicates wrong object type
                ValkeyModule_ReplyWithNull(ctx);
            } else {
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
            }
        }
    }
}

int Command_JsonStrAppend(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    const char *json;
    size_t json_len;
    JsonUtilCode rc = parseStrAppendCmdArgs(argv, argc, &key_str, &path, &json, &json_len);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);
    CHECK_DOCUMENT_SIZE_LIMIT(ctx, orig_doc_size + json_len)

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    // do string append
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_string_append(doc, path, json, json_len, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_update(doc, orig_doc_size, new_doc_size, json_len);

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.strappend", key_str);

    reply_strappend(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for JSON.TOGGLE.
 */
STATIC void reply_toggle(jsn::vector<int> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (*it != -1) {  // -1 means the value is not boolean
                // convert the result to string
                const char *buf = (*it == 1? "true" : "false");
                ValkeyModule_ReplyWithStringBuffer(ctx, buf, strlen(buf));
                return;
            }
        }
        // It's impossible to reach here, because the upstream method has verified there is at least
        // one boolean value.
        ValkeyModule_Assert(false);
    } else {
        // JSONPath: return an array of new values.
        //  0 - false
        //  1 - true
        // -1 - the value is not boolean, corresponding return value is null.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if ((*it) == -1) {
                ValkeyModule_ReplyWithNull(ctx);
            } else {
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
            }
        }
    }
}

int Command_JsonToggle(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;

    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS)
            return ValkeyModule_WrongArity(ctx);
        else
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // toggle the boolean value at this path
    jsn::vector<int> vec;
    bool is_v2_path;
    rc = dom_toggle(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.toggle", key_str);

    reply_toggle(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

int Command_JsonObjLen(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get object length
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_object_length(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    reply_strlen_objlen(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for JSON.OBJKEYS.
 */
STATIC void reply_objkeys(jsn::vector<jsn::vector<jsn::string>> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return an array of keys.
        // If there are multiple objects, return the keys of the first object.
        if (vec.empty()) {
            ValkeyModule_ReplyWithEmptyArray(ctx);
        } else {
            for (auto it = vec.begin(); it != vec.end(); it++) {
                if (!it->empty()) {
                    ValkeyModule_ReplyWithArray(ctx, it->size());
                    for (jsn::string &key : *it) {
                        ValkeyModule_ReplyWithStringBuffer(ctx, key.c_str(), key.length());
                    }
                    return;
                }
            }
            ValkeyModule_ReplyWithEmptyArray(ctx);
        }
    } else {
        // JSONPath: return an array of array of keys.
        // In the first level vector, number of items is number of objects. In the second level vector, number of
        // items is number keys in the object. If an object has no keys, its corresponding return value is empty array.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (it->empty()) {
                ValkeyModule_ReplyWithEmptyArray(ctx);
            } else {
                ValkeyModule_ReplyWithArray(ctx, it->size());
                for (jsn::string &key : *it) {
                    ValkeyModule_ReplyWithStringBuffer(ctx, key.c_str(), key.length());
                }
            }
        }
    }
}

int Command_JsonObjKeys(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get object keys
    jsn::vector<jsn::vector<jsn::string>> vec;
    bool is_v2_path;
    rc = dom_object_keys(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_INVALID_JSON_PATH || rc == JSONUTIL_JSON_PATH_NOT_EXIST)
            return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    reply_objkeys(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for some array commands.
 */
STATIC void reply_array_command(jsn::vector<size_t> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (*it != SIZE_MAX) {  // SIZE_MAX indicates wrong type
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
                return;
            }
        }
        // It's impossible to reach here, because the upstream method has verified there is at least
        // one array value.
        ValkeyModule_Assert(false);
    } else {
        // JSONPath: return an array of lengths.
        // If a value is SIZE_MAX, its corresponding element is null.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if ((*it) == SIZE_MAX) {  // SIZE_MAX indicates wrong type
                ValkeyModule_ReplyWithNull(ctx);
            } else {
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
            }
        }
    }
}

int Command_JsonArrLen(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get array length
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_array_length(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    reply_array_command(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

int Command_JsonArrAppend(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ArrAppendCmdArgs args;
    JsonUtilCode rc = parseArrAppendCmdArgs(ctx, argv, argc, &args);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, args.key, &key);
    if (rc != JSONUTIL_SUCCESS) {
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    // do array append
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_array_append(ctx, doc, args.path, args.jsons, args.json_len_arr, args.num_values, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_update(doc, orig_doc_size, new_doc_size, args.total_json_len);

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.arrappend", args.key);

    reply_array_command(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for JSON.ARRPOP.
 */
STATIC void reply_arrpop(jsn::vector<rapidjson::StringBuffer> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (it->GetLength() != 0) {  // emtpy indicates empty array or wrong type
                ValkeyModule_ReplyWithStringBuffer(ctx, it->GetString(), it->GetLength());
                return;
            }
        }
        ValkeyModule_ReplyWithNull(ctx);
    } else {
        // JSONPath: return an array of lengths.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (it->GetLength() == 0) {  // emtpy indicates empty array or wrong type
                ValkeyModule_ReplyWithNull(ctx);
            } else {
                ValkeyModule_ReplyWithStringBuffer(ctx, it->GetString(), it->GetLength());
            }
        }
    }
}

int Command_JsonArrPop(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    int64_t index;
    JsonUtilCode rc = parseArrPopCmdArgs(argv, argc, &key_str, &path, &index);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) {
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    // do array pop
    jsn::vector<rapidjson::StringBuffer> vec;
    bool is_v2_path;
    rc = dom_array_pop(doc, path, index, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_EMPTY_JSON_ARRAY) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_delete(doc, false, orig_doc_size, new_doc_size, abs(delta));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.arrpop", key_str);

    reply_arrpop(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

int Command_JsonArrInsert(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ArrInsertCmdArgs args;
    JsonUtilCode rc = parseArrInsertCmdArgs(ctx, argv, argc, &args);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, args.key, &key);
    if (rc != JSONUTIL_SUCCESS) {
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);

    // begin tracking
    int64_t begin_val = jsonstats_begin_track_mem();

    // do array insert
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_array_insert(ctx, doc, args.path, args.index, args.jsons, args.json_len_arr, args.num_values,
                          vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_insert(doc, false, orig_doc_size, new_doc_size, args.total_json_len);

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.arrinsert", args.key);

    reply_array_command(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

int Command_JsonArrTrim(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    int64_t start;
    int64_t stop;
    JsonUtilCode rc = parseArrTrimCmdArgs(argv, argc, &key_str, &path, &start, &stop);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) {
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    // do array trim
    jsn::vector<size_t> vec;
    bool is_v2_path;
    rc = dom_array_trim(doc, path, start, stop, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_delete(doc, false, orig_doc_size, new_doc_size, abs(delta));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.arrtrim", key_str);

    reply_array_command(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

int Command_JsonClear(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;

    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS)
            return ValkeyModule_WrongArity(ctx);
        else
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    size_t orig_doc_size = dom_get_doc_size(doc);

    // begin tracking memory
    int64_t begin_val = jsonstats_begin_track_mem();

    // do element clear
    size_t elements_cleared;
    rc = dom_clear(doc, path, elements_cleared);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    // end tracking memory
    int64_t delta = jsonstats_end_track_mem(begin_val);
    size_t new_doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, new_doc_size);

    // update stats
    jsonstats_update_stats_on_delete(doc, false, orig_doc_size, new_doc_size, abs(delta));

    // replicate the command
    ValkeyModule_ReplicateVerbatim(ctx);

    ValkeyModule_NotifyKeyspaceEvent(ctx, VALKEYMODULE_NOTIFY_GENERIC, "json.clear", key_str);
    return ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(elements_cleared));
}

/**
 * A helper method to send a reply to the client for JSON.ARRINDEX.
 */
STATIC void reply_arrindex(jsn::vector<int64_t> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if (*it != INT64_MAX) {  // INT64_MAX indicates wrong type
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
                return;
            }
        }
        // It's impossible to reach here, because the upstream method has verified there is at least
        // one array value.
        ValkeyModule_Assert(false);
    } else {
        // JSONPath: return an array of lengths.
        // If a value is SIZE_MAX, its corresponding element is null.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            if ((*it) == INT64_MAX) {  // INT64_MAX indicates wrong type
                ValkeyModule_ReplyWithNull(ctx);
            } else {
                ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*it));
            }
        }
    }
}

int Command_JsonArrIndex(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ArrIndexCmdArgs args;
    JsonUtilCode rc = parseArrIndexCmdArgs(argv, argc, &args);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, args.key, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // do array index of
    jsn::vector<int64_t> vec;
    bool is_v2_path;
    rc = dom_array_index_of(doc, args.path, args.scalar_val, args.scalar_val_len,
                            args.start, args.stop, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    reply_arrindex(vec, is_v2_path, ctx);
    return VALKEYMODULE_OK;
}

/**
 * A helper method to send a reply to the client for JSON.TYPE.
 */
STATIC int reply_type(jsn::vector<jsn::string> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        if (vec.empty()) {
            // It's impossible to reach here, because the upstream method has verified there is at least one value.
            ValkeyModule_Assert(false);
        } else {
            auto it = vec.begin();
            return ValkeyModule_ReplyWithSimpleString(ctx, it->c_str());
        }
    } else {
        // JSONPath: return an array of types.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            ValkeyModule_ReplyWithSimpleString(ctx, it->c_str());
        }
        return VALKEYMODULE_OK;
    }
}

int Command_JsonType(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get type of the value
    jsn::vector<jsn::string> vec;
    bool is_v2_path;
    rc = dom_value_type(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_INVALID_JSON_PATH || rc == JSONUTIL_JSON_PATH_NOT_EXIST)
            return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    return reply_type(vec, is_v2_path, ctx);
}

int Command_JsonResp(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    ValkeyModuleString *key_str;
    const char *path;
    JsonUtilCode rc = parseSimpleCmdArgs(argv, argc, &key_str, &path);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    ValkeyModuleKey *key;
    rc = verify_doc_key(ctx, key_str, &key, true);
    if (rc != JSONUTIL_SUCCESS) {
        if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
        return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
    }

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // reply with RESP protocol
    rc = dom_reply_with_resp(ctx, doc, path);
    if (rc != JSONUTIL_SUCCESS) return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));

    return VALKEYMODULE_OK;
}

STATIC JsonUtilCode processMemorySubCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, const int argc,
                                        jsn::vector<size_t> &vec, bool &is_v2_path) {
    ValkeyModuleKey *key;
    const char *path;
    bool default_path;
    JsonUtilCode rc = parseMemoryOrFieldsSubCmdArgs(ctx, argv, argc, &key, &path, default_path);
    if (rc != JSONUTIL_SUCCESS) return rc;

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // compute memory size of the JSON element
    return dom_mem_size(doc, path, vec, is_v2_path, default_path);
}

STATIC JsonUtilCode processFieldsSubCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, const int argc,
                                        jsn::vector<size_t> &vec, bool &is_v2_path) {
    ValkeyModuleKey *key;
    const char *path;
    bool default_path;
    JsonUtilCode rc = parseMemoryOrFieldsSubCmdArgs(ctx, argv, argc, &key, &path, default_path);
    if (rc != JSONUTIL_SUCCESS) return rc;

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get number of fields for the value
    return dom_num_fields(doc, path, vec, is_v2_path);
}

STATIC JsonUtilCode processDepthSubCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, const int argc, size_t *depth) {
    // there should be exactly 3 arguments
    if (argc != 3) return JSONUTIL_WRONG_NUM_ARGS;

    ValkeyModuleKey *key;
    JsonUtilCode rc = verify_doc_key(ctx, argv[2], &key, true);
    if (rc != JSONUTIL_SUCCESS) return rc;

    // fetch doc object from Valkey dict
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));

    // get max path depth of the doc
    dom_path_depth(doc, depth);
    return JSONUTIL_SUCCESS;
}

struct MaxDepthKey {
    MaxDepthKey() : max_depth(0), keyname() {}
    size_t max_depth;
    jsn::string keyname;
};

STATIC void scan_max_depth_key_callback(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname, ValkeyModuleKey *key,
                                        void *privdata) {
    VALKEYMODULE_NOT_USED(ctx);
    if (ValkeyModule_ModuleTypeGetType(key) == DocumentType) {
        JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
        size_t depth = 0;
        dom_path_depth(doc, &depth);
        MaxDepthKey *mdk = static_cast<MaxDepthKey*>(privdata);
        if (depth > mdk->max_depth) {
            mdk->max_depth = depth;
            const char *s = ValkeyModule_StringPtrLen(keyname, nullptr);
            mdk->keyname = jsn::string(s);
        }
    }
}

STATIC void processMaxDepthKeySubCmd(ValkeyModuleCtx *ctx, MaxDepthKey *mdk) {
    // scan keys
    ValkeyModuleScanCursor *cursor = ValkeyModule_ScanCursorCreate();
    while (ValkeyModule_Scan(ctx, cursor, scan_max_depth_key_callback, mdk)) {}
    ValkeyModule_ScanCursorDestroy(cursor);
}

struct MaxSizeKey {
    MaxSizeKey() : max_size(0), keyname() {}
    size_t max_size;
    jsn::string keyname;
};

STATIC void scan_max_size_key_callback(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname, ValkeyModuleKey *key,
                                       void *privdata) {
    VALKEYMODULE_NOT_USED(ctx);
    if (ValkeyModule_ModuleTypeGetType(key) == DocumentType) {
        JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
        size_t size = dom_get_doc_size(doc);
        MaxSizeKey *msk = static_cast<MaxSizeKey*>(privdata);
        if (size > msk->max_size) {
            msk->max_size = size;
            const char *s = ValkeyModule_StringPtrLen(keyname, nullptr);
            msk->keyname = jsn::string(s);
        }
    }
}

STATIC void processMaxSizeKeySubCmd(ValkeyModuleCtx *ctx, MaxSizeKey *msk) {
    // scan keys
    ValkeyModuleScanCursor *cursor = ValkeyModule_ScanCursorCreate();
    while (ValkeyModule_Scan(ctx, cursor, scan_max_size_key_callback, msk)) {}
    ValkeyModule_ScanCursorDestroy(cursor);
}

struct KeyTableValidate {
    std::unordered_map<const KeyTable_Layout *, size_t> counts;
    size_t handles = 0;
    void walk_json(JValue &v) {
        if (v.IsObject()) {
            ValkeyModule_Log(nullptr, "debug", "Found Object");
            for (JValue::MemberIterator m = v.MemberBegin(); m != v.MemberEnd(); ++m) {
                ValkeyModule_Log(nullptr, "debug", "Found Member : %s", m->name->getText());
                counts[&*(m->name)]++;
                handles++;
                walk_json(m->value);
            }
        } else if (v.IsArray()) {
            for (size_t i = 0; i < v.Size(); ++i) {
                walk_json(v[i]);
            }
        }
    }
};

STATIC void keytable_validate(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname, ValkeyModuleKey *key, void *privdata) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(keyname);
    auto ktv = reinterpret_cast<KeyTableValidate *>(privdata);
    if (ValkeyModule_ModuleTypeGetType(key) == DocumentType) {
        JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
        ktv->walk_json(doc->GetJValue());
    }
}

STATIC std::string processKeytableCheckCmd(ValkeyModuleCtx *ctx, size_t *handles, size_t *keys) {
    KeyTableValidate validate;
    //
    // Step 1, walk all of the keys in all of the databases, gathering the current reference counts for each key
    //
    int OriginalDb = ValkeyModule_GetSelectedDb(ctx);
    int dbnum = 0;
    while (ValkeyModule_SelectDb(ctx, dbnum) == VALKEYMODULE_OK) {
        ValkeyModuleScanCursor *cursor = ValkeyModule_ScanCursorCreate();
        while (ValkeyModule_Scan(ctx, cursor, keytable_validate, &validate)) {}
        ValkeyModule_ScanCursorDestroy(cursor);
        dbnum++;
    }
    ValkeyModule_Assert(ValkeyModule_SelectDb(ctx, OriginalDb) == VALKEYMODULE_OK);
    *handles = validate.handles;
    *keys = validate.counts.size();
    //
    // See if we agree on the overall totals
    //
    KeyTable::Stats stats = keyTable->getStats();
    if (stats.handles != validate.handles || stats.size != validate.counts.size()) {
        std::ostringstream os;
        os << "Mismatch on totals: Found: Handles:" << validate.handles << " & " << validate.counts.size()
            << " Expected: " << stats.handles << " & " << stats.size;
        return os.str();
    }
    //
    // Step 2, for each key, check the reference count against the KeyTable
    //
    return keyTable->validate_counts(validate.counts);
}

/**
 * A helper method to send a reply to the client for JSON.DEBUG MEMORY | FIELDS.
 */
STATIC int reply_debug_memory_fields(jsn::vector<size_t> &vec, const bool is_v2_path, ValkeyModuleCtx *ctx) {
    if (!is_v2_path) {
        // Legacy path: return a single value, which is the first value.
        if (vec.empty()) {
            // It's impossible to reach here, because the upstream method has verified there is at least one value.
            ValkeyModule_Assert(false);
        } else {
            auto it = vec.begin();
            return ValkeyModule_ReplyWithLongLong(ctx, *it);
        }
    } else {
        // JSONPath: return an array of integers.
        ValkeyModule_ReplyWithArray(ctx, vec.size());
        for (auto it = vec.begin(); it != vec.end(); it++) {
            ValkeyModule_ReplyWithLongLong(ctx, *it);
        }
        return VALKEYMODULE_OK;
    }
}

int Command_JsonDebug(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    ValkeyModule_AutoMemory(ctx);

    if (argc < 2) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;
        }
        return ValkeyModule_WrongArity(ctx);
    }

    const char *subcmd = ValkeyModule_StringPtrLen(argv[1], nullptr);
    if (!strcasecmp(subcmd, "MEMORY")) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            if (argc < 3) {
                return VALKEYMODULE_ERR;
            } else {
                ValkeyModule_KeyAtPos(ctx, 2);
                return VALKEYMODULE_OK;
            }
        }
        jsn::vector<size_t> vec;
        bool is_v2_path;
        JsonUtilCode rc = processMemorySubCmd(ctx, argv, argc, vec, is_v2_path);
        if (rc != JSONUTIL_SUCCESS) {
            if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
            if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }
        return reply_debug_memory_fields(vec, is_v2_path, ctx);
    } else if (!strcasecmp(subcmd, "FIELDS")) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            if (argc < 3) {
                return VALKEYMODULE_ERR;
            } else {
                ValkeyModule_KeyAtPos(ctx, 2);
                return VALKEYMODULE_OK;
            }
        }
        jsn::vector<size_t> vec;
        bool is_v2_path;
        JsonUtilCode rc = processFieldsSubCmd(ctx, argv, argc, vec, is_v2_path);
        if (rc != JSONUTIL_SUCCESS) {
            if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
            if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }
        return reply_debug_memory_fields(vec, is_v2_path, ctx);
    } else if (!strcasecmp(subcmd, "DEPTH")) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            if (argc < 3) {
                return VALKEYMODULE_ERR;
            } else {
                ValkeyModule_KeyAtPos(ctx, 2);
                return VALKEYMODULE_OK;
            }
        }
        size_t depth = 0;
        JsonUtilCode rc = processDepthSubCmd(ctx, argv, argc, &depth);
        if (rc != JSONUTIL_SUCCESS) {
            if (rc == JSONUTIL_WRONG_NUM_ARGS) return ValkeyModule_WrongArity(ctx);
            if (rc == JSONUTIL_DOCUMENT_KEY_NOT_FOUND) return ValkeyModule_ReplyWithNull(ctx);
            return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(rc));
        }
        return ValkeyModule_ReplyWithLongLong(ctx, depth);
    } else if (!strcasecmp(subcmd, "MAX-DEPTH-KEY")) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;;
        }

        // ATTENTION:
        // THIS IS AN UNDOCUMENTED SUBCOMMAND, TO BE USED FOR DEV TEST ONLY. DON'T RUN IT ON A PRODUCTION SYSTEM.
        // KEY SCAN MAY RUN FOR A LONG TIME LOCKING OUT ALL OTHER CLIENTS.

        // there should be exactly 2 arguments
        if (argc != 2) return ValkeyModule_WrongArity(ctx);

        MaxDepthKey mdk;
        processMaxDepthKeySubCmd(ctx, &mdk);
        ValkeyModule_ReplyWithArray(ctx, 2);
        ValkeyModule_ReplyWithLongLong(ctx, mdk.max_depth);
        ValkeyModule_ReplyWithSimpleString(ctx, mdk.keyname.c_str());
        return VALKEYMODULE_OK;
    } else if (!strcasecmp(subcmd, "MAX-SIZE-KEY")) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;
        }

        // ATTENTION:
        // THIS IS AN UNDOCUMENTED SUBCOMMAND, TO BE USED FOR DEV TEST ONLY. DON'T RUN IT ON A PRODUCTION SYSTEM.
        // KEY SCAN MAY RUN FOR A LONG TIME LOCKING OUT ALL OTHER CLIENTS.

        // there should be exactly 2 arguments
        if (argc != 2) return ValkeyModule_WrongArity(ctx);

        MaxSizeKey msk;
        processMaxSizeKeySubCmd(ctx, &msk);
        ValkeyModule_ReplyWithArray(ctx, 2);
        ValkeyModule_ReplyWithLongLong(ctx, msk.max_size);
        ValkeyModule_ReplyWithSimpleString(ctx, msk.keyname.c_str());
        return VALKEYMODULE_OK;
    } else if (!strcasecmp(subcmd, "KEYTABLE-CHECK")) {
        // Validate that all use-counts of KeyTable are correct
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;
        }

        // ATTENTION:
        // THIS IS AN UNDOCUMENTED SUBCOMMAND, TO BE USED FOR DEV TEST ONLY. DON'T RUN IT ON A PRODUCTION SYSTEM.
        // KEY SCAN MAY RUN FOR A LONG TIME LOCKING OUT ALL OTHER CLIENTS.

        // there should be exactly 2 arguments
        if (argc != 2) return ValkeyModule_WrongArity(ctx);

        size_t handles, keys;
        std::string error_message = processKeytableCheckCmd(ctx, &handles, &keys);
        if (error_message.length() > 0) {
            return ValkeyModule_ReplyWithError(ctx, error_message.c_str());
        } else {
            ValkeyModule_Log(ctx, "info", "KeyTableCheck completed ok, Handles:%zu, Keys:%zu", handles, keys);
            ValkeyModule_ReplyWithArray(ctx, 2);
            ValkeyModule_ReplyWithLongLong(ctx, handles);
            ValkeyModule_ReplyWithLongLong(ctx, keys);
            return VALKEYMODULE_OK;
        }
    } else if (!strcasecmp(subcmd, "KEYTABLE-CORRUPT")) {
        // Validate that all use-counts of KeyTable are correct
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;
        }

        // ATTENTION:
        // THIS IS AN UNDOCUMENTED SUBCOMMAND, TO BE USED FOR DEV TEST ONLY. DON'T RUN IT ON A PRODUCTION SYSTEM.
        //



        // there should be exactly 3 arguments
        if (argc != 3) return ValkeyModule_WrongArity(ctx);

        size_t len;
        const char *str = ValkeyModule_StringPtrLen(argv[2], &len);

        KeyTable_Handle h = keyTable->makeHandle(str, len);
        ValkeyModule_Log(ctx, "warning", "*** Handle %s count is now %zd", str, h->getRefCount());
        return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    } else if (!strcasecmp(subcmd, "KEYTABLE-DISTRIBUTION")) {
        // compute longest runs of non-empty hashtable entries, a direct measure of key distribution and
        // worst-case run-time for lookup/insert/delete
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;
        }

        // ATTENTION:
        // THIS IS AN UNDOCUMENTED SUBCOMMAND, DON'T RUN IT ON A PRODUCTION SYSTEM
        // UNLESS YOU KNOW WHAT YOU'RE DOING -- IT CAN LOCK THE MAINTHREAD FOR SEVERAL SECONDS
        //



        // there should be exactly 3 arguments
        if (argc != 3) return ValkeyModule_WrongArity(ctx);
        size_t topN = atol(ValkeyModule_StringPtrLen(argv[2], nullptr));

        KeyTable::LongStats ls = keyTable->getLongStats(topN);
        ValkeyModule_ReplyWithArray(ctx, 2 * ls.runs.size());
        for (auto it = ls.runs.rbegin(); it != ls.runs.rend(); ++it) {
            ValkeyModule_ReplyWithLongLong(ctx, it->first);
            ValkeyModule_ReplyWithLongLong(ctx, it->second);
        }
        return VALKEYMODULE_OK;
    } else if (!strcasecmp(subcmd, "HELP")) {
        if (ValkeyModule_IsKeysPositionRequest(ctx)) {
            return VALKEYMODULE_ERR;
        }

        // there should be exactly 2 arguments
        if (argc != 2) return ValkeyModule_WrongArity(ctx);

        std::vector<std::string> cmds;
        cmds.push_back("JSON.DEBUG MEMORY <key> [path] - report memory size (bytes). "
                       "Without path reports document size without keys. "
                       "With path reports size including keys");
        cmds.push_back("JSON.DEBUG DEPTH <key> - report the maximum path depth of the JSON document.");
        cmds.push_back("JSON.DEBUG FIELDS <key> [path] - report number of fields in the "
                       "JSON element. Path defaults to root if not provided.");
        cmds.push_back("JSON.DEBUG HELP - print help message.");
        cmds.push_back("------- DANGER, LONG RUNNING COMMANDS, DON'T USE ON PRODUCTION SYSTEM --------");
        cmds.push_back("JSON.DEBUG MAX-DEPTH-KEY - Find JSON key with maximum depth");
        cmds.push_back("JSON.DEBUG MAX-SIZE-KEY  - Find JSON key with largest memory size");
        cmds.push_back("JSON.DEBUG KEYTABLE-CHECK - Extended KeyTable integrity check");
        cmds.push_back("JSON.DEBUG KEYTABLE-CORRUPT <name> - Intentionally corrupt KeyTable handle counts");
        cmds.push_back("JSON.DEBUG KEYTABLE-DISTRIBUTION <topN> - Find and count topN longest runs in KeyTable");

        ValkeyModule_ReplyWithArray(ctx, cmds.size());
        for (auto& s : cmds) ValkeyModule_ReplyWithSimpleString(ctx, s.c_str());
        return VALKEYMODULE_OK;
    }

    return ValkeyModule_ReplyWithError(ctx, jsonutil_code_to_message(JSONUTIL_UNKNOWN_SUBCOMMAND));
}

/* =========================== Callback Methods =========================== */

/*
 * Load an OBJECT using the IO machinery.
 */
void *DocumentType_RdbLoad(ValkeyModuleIO *rdb, int encver) {
    if (encver > DOCUMENT_TYPE_ENCODING_VERSION) {
        ValkeyModule_LogIOError(rdb, "warning",
                               "Cannot load document type version %d, because current module's document version is %d.",
                               encver, DOCUMENT_TYPE_ENCODING_VERSION);
        return nullptr;
    }

    // begin tracking memory
    JDocument *doc;
    int64_t begin_val = jsonstats_begin_track_mem();
    JsonUtilCode rc = dom_load(&doc, rdb, encver);
    int64_t delta = jsonstats_end_track_mem(begin_val);
    if (rc != JSONUTIL_SUCCESS) {
        ValkeyModule_Assert(delta == 0);
        return nullptr;
    }
    // end tracking memory
    size_t doc_size = dom_get_doc_size(doc) + delta;
    dom_set_doc_size(doc, doc_size);

    // update stats
    jsonstats_update_stats_on_insert(doc, true, 0, doc_size, doc_size);
    return doc;
}

/*
 * Save the Document. Convert it into chunks, write them and then write an EOF marker
 */
void DocumentType_RdbSave(ValkeyModuleIO *rdb, void *value) {
    JDocument *doc = static_cast<JDocument*>(value);
    dom_save(doc, rdb, DOCUMENT_TYPE_ENCODING_VERSION);
    //
    // Let's make sure any I/O error generates a log entry
    //
    if (ValkeyModule_IsIOError(rdb)) {
        ValkeyModule_LogIOError(rdb, "warning", "Unable to save JSON object, I/O error");
    }
}

void *DocumentType_Copy(ValkeyModuleString *from_key_name, ValkeyModuleString *to_key_name,
                                   const void *value) {
    VALKEYMODULE_NOT_USED(from_key_name);  // We don't care about the from/to key names.
    VALKEYMODULE_NOT_USED(to_key_name);
    const JDocument *source = static_cast<const JDocument*>(value);
    JDocument *doc = dom_copy(source);
    // Treat this the same as JSON.SET <key> .
    size_t doc_size = dom_get_doc_size(doc);
    jsonstats_update_stats_on_insert(doc, true, 0, doc_size, doc_size);
    return doc;
}

/*
 * Defrag callback.
 * If the JSON object size is less than or equal to the defrag threshold, the JSON object is
 * re-allocated. The re-allocation is done by copying the original object into a new one,
 * swapping them, and deleting the original one. Note that the current implementation does not
 * support defrag stop and resume, which is needed for very large JSON objects.
 */
int DocumentType_Defrag(ValkeyModuleDefragCtx *ctx, ValkeyModuleString *key, void **value) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(key);
    ValkeyModule_Assert(*value != nullptr);
    JDocument *orig = static_cast<JDocument*>(*value);
    size_t doc_size = dom_get_doc_size(orig);
    // We do not want to defrag a key larger than the default max document size.
    // If there is a need to do that, increase the defrag-threshold config value.
    if (doc_size <= json_get_defrag_threshold()) {
        JDocument *new_doc = dom_copy(orig);
        dom_set_bucket_id(new_doc, dom_get_bucket_id(orig));
        *value = new_doc;
        dom_free_doc(orig);  // free the original value
        jsonstats_increment_defrag_count();
        jsonstats_increment_defrag_bytes(doc_size);
    }
    return 0;
}

void DocumentType_AofRewrite(ValkeyModuleIO *aof, ValkeyModuleString *key, void *value) {
    JDocument *doc = static_cast<JDocument*>(value);
    rapidjson::StringBuffer oss;
    dom_serialize(doc, nullptr, oss);
    ValkeyModule_EmitAOF(aof, "JSON.SET", "scc", key, ".", oss.GetString());
}

void DocumentType_Free(void *value) {
    JDocument *doc = static_cast<JDocument*>(value);
    size_t orig_doc_size = dom_get_doc_size(doc);

    // update stats
    jsonstats_update_stats_on_delete(doc, true, orig_doc_size, 0, orig_doc_size);

    dom_free_doc(doc);
}

size_t DocumentType_MemUsage(const void *value) {
    const JDocument *doc = static_cast<const JDocument*>(value);
    return dom_get_doc_size(doc);
}

// NOTE: Valkey will prefix every section and field name with the module name.
void Module_Info(ValkeyModuleInfoCtx *ctx, int for_crash_report) {
    VALKEYMODULE_NOT_USED(for_crash_report);

    // section: core metrics
#define beginSection(name) \
    if (ValkeyModule_InfoAddSection(ctx, const_cast<char *>(name)) != VALKEYMODULE_ERR) {
#define endSection() }

#define addULongLong(name, value) { \
    if (ValkeyModule_InfoAddFieldULongLong(ctx, const_cast<char *>(name), value) == VALKEYMODULE_ERR) { \
        ValkeyModule_Log(nullptr, "warning", "Can't add info variable %s", name); \
    } \
}

#define addDouble(name, value) { \
    if (ValkeyModule_InfoAddFieldDouble(ctx, const_cast<char *>(name), value) == VALKEYMODULE_ERR) { \
        ValkeyModule_Log(nullptr, "warning", "Can't add info variable %s", name); \
    } \
}


    //
    // User visible metrics
    //
    beginSection("core_metrics")
        addULongLong("total_memory_bytes", jsonstats_get_used_mem() + keyTable->getStats().bytes);
        addULongLong("num_documents", jsonstats_get_num_doc_keys());
    endSection();

    beginSection("ext_metrics")
        addULongLong("max_path_depth_ever_seen", jsonstats_get_max_depth_ever_seen());
        addULongLong("max_document_size_ever_seen", jsonstats_get_max_size_ever_seen());
        addULongLong("total_malloc_bytes_used", memory_usage());
        addULongLong("memory_traps_enabled", memory_traps_enabled());
        addULongLong("defrag_count", jsonstats_get_defrag_count());
        addULongLong("defrag_bytes", jsonstats_get_defrag_bytes());
    endSection();

    beginSection("document_composition")
        addULongLong("boolean_count", logical_stats.boolean_count);
        addULongLong("number_count", logical_stats.number_count);
        addULongLong("sum_extra_numeric_chars", logical_stats.sum_extra_numeric_chars);
        addULongLong("string_count", logical_stats.string_count);
        addULongLong("sum_string_chars", logical_stats.sum_string_chars);
        addULongLong("null_count", logical_stats.null_count);
        addULongLong("array_count", logical_stats.array_count);
        addULongLong("sum_array_elements", logical_stats.sum_array_elements);
        addULongLong("object_count", logical_stats.object_count);
        addULongLong("sum_object_members", logical_stats.sum_object_members);
        addULongLong("sum_object_key_chars", logical_stats.sum_object_key_chars);
    endSection();

    // section: histograms
    beginSection("histograms")
        char name[128];
        char buf[1024];
        snprintf(name, sizeof(name), "doc_histogram");
        jsonstats_sprint_doc_hist(buf, sizeof(buf));
        ValkeyModule_InfoAddFieldCString(ctx, name, buf);

        snprintf(name, sizeof(name), "read_histogram");
        jsonstats_sprint_read_hist(buf, sizeof(buf));
        ValkeyModule_InfoAddFieldCString(ctx, name, buf);

        snprintf(name, sizeof(name), "insert_histogram");
        jsonstats_sprint_insert_hist(buf, sizeof(buf));
        ValkeyModule_InfoAddFieldCString(ctx, name, buf);

        snprintf(name, sizeof(name), "update_histogram");
        jsonstats_sprint_update_hist(buf, sizeof(buf));
        ValkeyModule_InfoAddFieldCString(ctx, name, buf);

        snprintf(name, sizeof(name), "delete_histogram");
        jsonstats_sprint_delete_hist(buf, sizeof(buf));
        ValkeyModule_InfoAddFieldCString(ctx, name, buf);

        snprintf(name, sizeof(name), "histogram_buckets");
        jsonstats_sprint_hist_buckets(buf, sizeof(buf));
        ValkeyModule_InfoAddFieldCString(ctx, name, buf);
    endSection();
}

//
// Change a KeyTable parameter. Validate the change first.
//
int handleFactor(float KeyTable::Factors::*f, const void *v, const char *name) {
    float value = *reinterpret_cast<const int *>(v) / 100.0f;
    KeyTable::Factors factors;
    factors = keyTable->getFactors();
    float oldvalue = factors.*f;
    factors.*f = value;
    const char *validity = KeyTable::isValidFactors(factors);
    if (validity == nullptr) {
        keyTable->setFactors(factors);
        ValkeyModule_Log(nullptr, "debug", "Set %s to %f (was %f)", name, double(value), double(oldvalue));
        return VALKEYMODULE_OK;
    } else {
        ValkeyModule_Log(nullptr, "warning", "Error setting parameter %s to %g",
                        validity, double(value));
        return VALKEYMODULE_ERR;
    }
}

//
// Change a HashTable parameter. Validate the change first
//
template<typename T>
int handleHashTableFactor(T rapidjson::HashTableFactors::*f, const void *v, T scale_factor) {
    int unscaled = *reinterpret_cast<const int *>(v);
    T value = unscaled / scale_factor;
    rapidjson::HashTableFactors h = rapidjson::hashTableFactors;
    h.*f = value;
    const char *validity = h.isValid();
    if (validity == nullptr) {
        rapidjson::hashTableFactors = h;
        return VALKEYMODULE_OK;
    } else {
        ValkeyModule_Log(nullptr, "warning", "Error setting parameter %s from (unscaled: %d)",
                        validity, unscaled);
        return VALKEYMODULE_ERR;
    }
}

//
// Resize the number of shards in the keyTable. this isn't multi-thread safe. But the current AppConfig architecture
// doesn't provide a good way to solve this problem. Also, we only do it when the table is empty. As long as there
// are no background operations in progress (slot migration, threadsave) we're good. Sadly there's no easy way for
// a module to detect that. Once we have RM_ApplyConfig, we'll restrict this to only happen at initialization time.
// and close this small timing hole.
//

KeyTable::Factors destroyKeyTable() {
    KeyTable::Factors factors;
    factors = keyTable->getFactors();
    keyTable->~KeyTable();
    memory_free(keyTable);
    keyTable = nullptr;
    return factors;
}

void initKeyTable(unsigned numShards, KeyTable::Factors factors) {
    ValkeyModule_Assert(keyTable == nullptr);
    ValkeyModule_Log(nullptr, "debug", "Setting shards to %d", numShards);
    KeyTable::Config config;
    config.malloc = memory_alloc;
    config.free = memory_free;
    config.hash = hash_function;
    config.numShards = numShards;
    keyTable = new(memory_alloc(sizeof(KeyTable))) KeyTable(config);
    keyTable->setFactors(factors);
}

//
// Handle "config set json.key-table-num-shards"
//
int handleSetNumShards(const void *v) {
    int value = *reinterpret_cast<const int *>(v);
    auto s = keyTable->getStats();
    if (s.handles != 0) {
        ValkeyModule_Log(nullptr, "warning", "Can't change numShards after initialization");
        return VALKEYMODULE_ERR;
    }
    if (value < KeyTable::MIN_SHARDS || value > KeyTable::MAX_SHARDS) {
        ValkeyModule_Log(nullptr, "warning", "numShards value out of range");
        return VALKEYMODULE_ERR;
    }
    initKeyTable(value, destroyKeyTable());
    return VALKEYMODULE_OK;
}

int Config_GetInstrumentEnabled(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    return *static_cast<int*>(privdata);
}

int Config_SetInstrumentEnabled(const char *name, int val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    *static_cast<int*>(privdata) = val;
    return VALKEYMODULE_OK;
}

//
// Handle "config set json.enable-memory-traps"
//
int Config_GetMemoryTrapsEnable(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return memory_traps_enabled();
}

int Config_SetMemoryTrapsEnable(const char *name, int value, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    VALKEYMODULE_NOT_USED(privdata);
    ValkeyModule_Log(nullptr, "warning", "Changing memory traps to %d", value);
    size_t num_json_keys = jsonstats_get_num_doc_keys();
    auto s = keyTable->getStats();
    if (num_json_keys > 0 || s.handles != 0) {
        static char errmsg[] = "Can't change memory traps with JSON data present";
        *err = ValkeyModule_CreateString(nullptr, errmsg, strlen(errmsg));
        ValkeyModule_Log(nullptr, "warning", "Can't change memory traps with %zu JSON keys present", num_json_keys);
        return VALKEYMODULE_ERR;
    }
    auto shards = keyTable->getNumShards();
    auto factors = destroyKeyTable();
    ValkeyModule_Assert(memory_usage() == 0);
    ValkeyModule_Assert(memory_traps_control(value));
    initKeyTable(shards, factors);
    return VALKEYMODULE_OK;
}

int Config_GetEnforceRdbVersionCheck(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    return *static_cast<bool*>(privdata)? 1 : 0;
}

int Config_SetEnforceRdbVersionCheck(const char *name, int val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    *static_cast<bool*>(privdata) = (val == 1);
    return VALKEYMODULE_OK;
}

long long Config_GetSizeConfig(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    return *static_cast<size_t*>(privdata);
}

int Config_SetSizeConfig(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(err);
    *static_cast<size_t*>(privdata) = val;
    return VALKEYMODULE_OK;
}

long long Config_GetKeyTableGrowFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return keyTable->getStats().factors.grow * 100;
}

int Config_SetKeyTableGrowFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleFactor(&KeyTable::Factors::grow, &val, name);
}

long long Config_GetKeyTableShrinkFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return keyTable->getStats().factors.shrink * 100;
}

int Config_SetKeyTableShrinkFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleFactor(&KeyTable::Factors::shrink, &val, name);
}

long long Config_GetKeyTableMinLoadFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return keyTable->getStats().factors.minLoad * 100;
}

int Config_SetKeyTableMinLoadFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleFactor(&KeyTable::Factors::minLoad, &val, name);
}

long long Config_GetKeyTableMaxLoadFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return keyTable->getStats().factors.maxLoad * 100;
}

int Config_SetKeyTableMaxLoadFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleFactor(&KeyTable::Factors::maxLoad, &val, name);
}

long long Config_GetKeyTableNumShards(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return keyTable->getNumShards();
}

int Config_SetKeyTableNumShards(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleSetNumShards(&val);
}

long long Config_GetHashTableGrowFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return rapidjson::hashTableFactors.grow * 100;
}

int Config_SetHashTableGrowFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleHashTableFactor(&rapidjson::HashTableFactors::grow, &val, 100.f);
}

long long Config_GetHashTableShrinkFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return rapidjson::hashTableFactors.shrink * 100;
}

int Config_SetHashTableShrinkFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleHashTableFactor(&rapidjson::HashTableFactors::shrink, &val, 100.f);
}

long long Config_GetHashTableMinLoadFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return rapidjson::hashTableFactors.minLoad * 100;
}

int Config_SetHashTableMinLoadFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleHashTableFactor(&rapidjson::HashTableFactors::minLoad, &val, 100.f);
}

long long Config_GetHashTableMaxLoadFactor(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return rapidjson::hashTableFactors.maxLoad * 100;
}

int Config_SetHashTableMaxLoadFactor(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleHashTableFactor(&rapidjson::HashTableFactors::maxLoad, &val, 100.f);
}

long long Config_GetHashTableMinSize(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return rapidjson::hashTableFactors.minHTSize;
}

int Config_SetHashTableMinSize(const char *name, long long val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    return handleHashTableFactor(&rapidjson::HashTableFactors::minHTSize, &val, size_t(1));
}

int registerModuleConfigs(ValkeyModuleCtx *ctx) {
    REGISTER_BOOL_CONFIG(ctx, "enable-memory-traps", 0, nullptr,
                         Config_GetMemoryTrapsEnable, Config_SetMemoryTrapsEnable)
    REGISTER_BOOL_CONFIG(ctx, "enable-instrument-insert", 0, &instrument_enabled_insert,
                         Config_GetInstrumentEnabled, Config_SetInstrumentEnabled)
    REGISTER_BOOL_CONFIG(ctx, "enable-instrument-update", 0, &instrument_enabled_update,
                         Config_GetInstrumentEnabled, Config_SetInstrumentEnabled)
    REGISTER_BOOL_CONFIG(ctx, "enable-instrument-delete", 0, &instrument_enabled_delete,
                         Config_GetInstrumentEnabled, Config_SetInstrumentEnabled)
    REGISTER_BOOL_CONFIG(ctx, "enable-instrument-dump-doc-before", 0,
                         &instrument_enabled_dump_doc_before,
                         Config_GetInstrumentEnabled, Config_SetInstrumentEnabled)
    REGISTER_BOOL_CONFIG(ctx, "enable-instrument-dump-doc-after", 0,
                         &instrument_enabled_dump_doc_after,
                         Config_GetInstrumentEnabled, Config_SetInstrumentEnabled)
    REGISTER_BOOL_CONFIG(ctx, "enable-instrument-dump-value-before-delete", 0,
                         &instrument_enabled_dump_value_before_delete,
                         Config_GetInstrumentEnabled, Config_SetInstrumentEnabled)
    REGISTER_BOOL_CONFIG(ctx, "enforce-rdb-version-check", 0, &enforce_rdb_version_check,
                         Config_GetEnforceRdbVersionCheck, Config_SetEnforceRdbVersionCheck)

    REGISTER_NUMERIC_CONFIG(ctx, "max-document-size", DEFAULT_MAX_DOCUMENT_SIZE, VALKEYMODULE_CONFIG_MEMORY, 0,
                            LLONG_MAX, &config_max_document_size, Config_GetSizeConfig, Config_SetSizeConfig)
    REGISTER_NUMERIC_CONFIG(ctx, "defrag-threshold", DEFAULT_DEFRAG_THRESHOLD, VALKEYMODULE_CONFIG_MEMORY, 0,
                            LLONG_MAX, &config_defrag_threshold, Config_GetSizeConfig, Config_SetSizeConfig)
    REGISTER_NUMERIC_CONFIG(ctx, "max-path-limit", 128, VALKEYMODULE_CONFIG_DEFAULT, 0, INT_MAX,
                            &config_max_path_limit, Config_GetSizeConfig, Config_SetSizeConfig)
    REGISTER_NUMERIC_CONFIG(ctx, "max-parser-recursion-depth", 200, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, &config_max_parser_recursion_depth, Config_GetSizeConfig,
                            Config_SetSizeConfig)
    REGISTER_NUMERIC_CONFIG(ctx, "max-recursive-descent-tokens", 20, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, &config_max_recursive_descent_tokens, Config_GetSizeConfig,
                            Config_SetSizeConfig)
    REGISTER_NUMERIC_CONFIG(ctx, "max-query-string-size", 128*1024, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, &config_max_query_string_size, Config_GetSizeConfig, Config_SetSizeConfig)

    REGISTER_NUMERIC_CONFIG(ctx, "key-table-grow-factor", 100, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetKeyTableGrowFactor, Config_SetKeyTableGrowFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "key-table-shrink-factor", 50, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetKeyTableShrinkFactor, Config_SetKeyTableShrinkFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "key-table-min-load-factor", 25, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetKeyTableMinLoadFactor, Config_SetKeyTableMinLoadFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "key-table-max-load-factor", 85, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetKeyTableMaxLoadFactor, Config_SetKeyTableMaxLoadFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "key-table-num-shards", 32768, VALKEYMODULE_CONFIG_DEFAULT, KeyTable::MIN_SHARDS,
                            KeyTable::MAX_SHARDS, nullptr, Config_GetKeyTableNumShards, Config_SetKeyTableNumShards)

    REGISTER_NUMERIC_CONFIG(ctx, "hash-table-grow-factor", 100, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetHashTableGrowFactor, Config_SetHashTableGrowFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "hash-table-shrink-factor", 50, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetHashTableShrinkFactor, Config_SetHashTableShrinkFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "hash-table-min-load-factor", 25, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetHashTableMinLoadFactor, Config_SetHashTableMinLoadFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "hash-table-max-load-factor", 85, VALKEYMODULE_CONFIG_DEFAULT, 0,
                            INT_MAX, nullptr, Config_GetHashTableMaxLoadFactor, Config_SetHashTableMaxLoadFactor)
    REGISTER_NUMERIC_CONFIG(ctx, "hash-table-min-size", 64, VALKEYMODULE_CONFIG_DEFAULT, 0, INT_MAX,
                            nullptr, Config_GetHashTableMinSize, Config_SetHashTableMinSize)

    ValkeyModule_LoadConfigs(ctx);
    return VALKEYMODULE_OK;
}

/*
 * Install stub datatype callback for aux_load.
 */
bool install_stub(ValkeyModuleCtx *ctx,
                  const char *type_name,
                  int encver,
                  int (*aux_load)(ValkeyModuleIO *, int encver, int when)) {
    ValkeyModuleTypeMethods type_methods;
    memset(&type_methods, 0, sizeof(ValkeyModuleTypeMethods));
    type_methods.version = VALKEYMODULE_TYPE_METHOD_VERSION;
    type_methods.aux_load = aux_load;
    if (ValkeyModule_CreateDataType(ctx, type_name, encver, &type_methods) == nullptr) {
        ValkeyModule_Log(ctx, "warning", "Failed to create data type %s", type_name);
        return false;
    }
    ValkeyModule_Log(ctx, "debug", "Successfully installed stub data type %s", type_name);
    return true;
}

/*
 * Check a string value, fail if the expected value isn't present.
 */
bool checkString(ValkeyModuleIO *ctx, const char *value, const char *caller) {
    size_t str_len;
    std::unique_ptr<char> str(ValkeyModule_LoadStringBuffer(ctx, &str_len));
    if (strncmp(value, str.get(), str_len)) {
        ValkeyModule_Log(nullptr, "warning", "%s: Unexpected value in RDB. Expected %s Received %s",
                        caller, value, str.get());
        return false;
    }
    return true;
}

/*
 * Check an integer value, fail
 */
bool checkInt(ValkeyModuleIO *ctx, uint64_t value, const char *caller) {
    uint64_t val = ValkeyModule_LoadUnsigned(ctx);
    if (value != val) {
        ValkeyModule_Log(nullptr, "warning", "%s: Unexpected value in RDB Expected: %lx Received: %lx",
                        caller, value, val);
        return false;
    }
    return true;
}

/*
 * Check the encoding version, For unsupported versions we ALWAYS put out a message in the log
 * but we only fail the RDB load if the config tells us to do it.
 */
bool checkVersion(const char *type_name, int encver, int expected_encver) {
    if (encver != expected_encver) {
        if (enforce_rdb_version_check) {
            ValkeyModule_Log(nullptr, "warning", "Unsupported Encoding Version %d for type:%s expected %d",
                            encver, type_name, expected_encver);
            return false;
        } else {
            ValkeyModule_Log(nullptr, "warning",
                            "Unsupported Encoding Version %d for type:%s expected %d, WILL ATTEMPT LOADING ANYWAYS",
                            encver, type_name, expected_encver);
        }
    }
    return true;
}

bool checkVersionRange(const char *type_name, int encver, int ver_low, int ver_high) {
    if (encver < ver_low || encver > ver_high) {
        if (enforce_rdb_version_check) {
            ValkeyModule_Log(nullptr, "warning", "Unsupported Encoding Version %d for type:%s expected [%d:%d]",
                            encver, type_name, ver_low, ver_high);
            return false;
        } else {
            ValkeyModule_Log(nullptr, "warning",
                            "Unsupported Encoding Version %d for type:%s expected [%d:%d],"
                            " WILL ATTEMPT TO LOAD ANYWAYS",
                            encver, type_name, ver_low, ver_high);
        }
    }
    return true;
}

/*
 * Stub for scdtype0 data type.
 */
#define SCDTYPE_ENCVER 1
int scdtype_aux_load(ValkeyModuleIO *ctx, int encver, int when) {
    if (!checkVersion("sdctype0", encver, SCDTYPE_ENCVER)) return VALKEYMODULE_ERR;
    if (when == VALKEYMODULE_AUX_AFTER_RDB) {
        if (!checkInt(ctx, 0, "scdtype")) return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

#define GEARSDT_ENCVER 3
int gearsdt_aux_load(ValkeyModuleIO *ctx, int encver, int when) {
    if (!checkVersion("gearsdt", encver, GEARSDT_ENCVER)) return VALKEYMODULE_ERR;
    if (when == VALKEYMODULE_AUX_AFTER_RDB) {
        if (!checkString(ctx, "StreamReader", "gears-dt")) return VALKEYMODULE_ERR;
        if (!checkInt(ctx, 0, "gears-dt")) return VALKEYMODULE_ERR;
        if (!checkString(ctx, "CommandReader", "gears-dt")) return VALKEYMODULE_ERR;
        if (!checkInt(ctx, 0, "gears-dt")) return VALKEYMODULE_ERR;
        if (!checkString(ctx, "KeysReader", "gears-dt")) return VALKEYMODULE_ERR;
        if (!checkInt(ctx, 0, "gears-dt")) return VALKEYMODULE_ERR;
        if (!checkString(ctx, "", "gears-dt")) return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

#define GEARSRQ_ENCVER 1
int gearsrq_aux_load(ValkeyModuleIO *ctx, int encver, int when) {
    if (!checkVersion("gearsrq", encver, GEARSRQ_ENCVER)) return VALKEYMODULE_ERR;
    if (when == VALKEYMODULE_AUX_BEFORE_RDB) {
        if (!checkInt(ctx, 0, "gearsrq")) return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

/*
 * The hash function is FNV-1a (See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)
 * We are looking for a 38-bit hash function. As recommended, we use the 64-bit FNV-1a constants and then
 * use XOR-folding to reduce the hash to 38 bits (as well as improving the randomness of the low order bit)
 */
size_t hash_function(const char *text, size_t length) {
    const unsigned char *t = reinterpret_cast<const unsigned char *>(text);
    size_t hsh = 14695981039346656037ull;
    for (size_t i = 0; i < length; ++i) {
        hsh = (hsh ^ t[i]) * 1099511628211ull;
    }
    //
    // Now reduce to 38-bits
    //
    return hsh ^ (hsh >> 38);
}

void DocumentType_Digest(ValkeyModuleDigest *ctx, void *vdoc) {
    JDocument *doc = reinterpret_cast<JDocument *>(vdoc);
    dom_compute_digest(ctx, doc);
}

bool set_command_info(ValkeyModuleCtx *ctx, const char *name, int32_t arity) {
    // Get command
    ValkeyModuleCommand *command = ValkeyModule_GetCommand(ctx, name);
    if (command == nullptr) {
        ValkeyModule_Log(ctx, "warning", "Failed to get command '%s'", name);
        return false;
    }
    ValkeyModuleCommandInfo info;
    memset(&info, 0, sizeof(info));
    info.version = VALKEYMODULE_COMMAND_INFO_VERSION;
    info.arity = arity;

    if (ValkeyModule_SetCommandInfo(command, &info) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command info for %s", name);
        return false;
    }
    return true;
}
// Overloaded method for setting key permissions
bool set_command_info(ValkeyModuleCtx *ctx, const char *name, int32_t arity, uint64_t keyspec_flags,
                        int bs_index, std::tuple<int, int, int> key_range) {
    // Get command
    ValkeyModuleCommand *command = ValkeyModule_GetCommand(ctx, name);
    if (command == nullptr) {
        ValkeyModule_Log(ctx, "warning", "Failed to get command '%s'", name);
        return false;
    }
    ValkeyModuleCommandInfo info;
    memset(&info, 0, sizeof(info));
    info.version = VALKEYMODULE_COMMAND_INFO_VERSION;
    info.arity = arity;

    // We only need one key_spec entry, but key_specs are sent as a null-entry terminated array,
    // so we leave a second value filled with 0s
    ValkeyModuleCommandKeySpec cmdKeySpec[2];
    memset(cmdKeySpec, 0, sizeof(cmdKeySpec));

    cmdKeySpec[0].flags = keyspec_flags;
    cmdKeySpec[0].begin_search_type = VALKEYMODULE_KSPEC_BS_INDEX;
    cmdKeySpec[0].bs.index = {bs_index};
    cmdKeySpec[0].find_keys_type = VALKEYMODULE_KSPEC_FK_RANGE;
    cmdKeySpec[0].fk.range = {std::get<0>(key_range), std::get<1>(key_range), std::get<2>(key_range)};

    info.key_specs = &cmdKeySpec[0];

    if (ValkeyModule_SetCommandInfo(command, &info) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command info for %s", name);
        return false;
    }
    return true;
}

/* ================================ Module OnLoad ============================= */

extern "C" int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx) {
    // Register the module
    if (ValkeyModule_Init(ctx, MODULE_NAME, MODULE_VERSION, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to initialize module %s version %d", MODULE_NAME, MODULE_VERSION);
        return VALKEYMODULE_ERR;
    }

    // Register module type callbacks
    ValkeyModuleTypeMethods type_methods;
    memset(&type_methods, 0, sizeof(ValkeyModuleTypeMethods));
    type_methods.version = VALKEYMODULE_TYPE_METHOD_VERSION;
    type_methods.rdb_load = DocumentType_RdbLoad;
    type_methods.rdb_save = DocumentType_RdbSave;
    type_methods.copy = DocumentType_Copy;
    type_methods.aof_rewrite = DocumentType_AofRewrite;
    type_methods.mem_usage = DocumentType_MemUsage;
    type_methods.free = DocumentType_Free;
    type_methods.digest = DocumentType_Digest;
    type_methods.defrag = DocumentType_Defrag;


    // Create module type
    DocumentType = ValkeyModule_CreateDataType(ctx, DOCUMENT_TYPE_NAME,
                                              DOCUMENT_TYPE_ENCODING_VERSION, &type_methods);
    if (DocumentType == nullptr) {
        ValkeyModule_Log(ctx, "warning", "Failed to create data type %s encver %d",
                        DOCUMENT_TYPE_NAME, DOCUMENT_TYPE_ENCODING_VERSION);
        return VALKEYMODULE_ERR;
    }

    /*
     * Now create the stub datatypes for search
     */
    if (!install_stub(ctx, "scdtype00", SCDTYPE_ENCVER, scdtype_aux_load)) return VALKEYMODULE_ERR;
    if (!install_stub(ctx, "GEARS_DT0", GEARSDT_ENCVER, gearsdt_aux_load)) return VALKEYMODULE_ERR;
    if (!install_stub(ctx, "GEAR_REQ0", GEARSRQ_ENCVER, gearsrq_aux_load)) return VALKEYMODULE_ERR;

    // Indicate that we can handle I/O errors ourself.
    ValkeyModule_SetModuleOptions(ctx, VALKEYMODULE_OPTIONS_HANDLE_IO_ERRORS);

    // Initialize metrics
    JsonUtilCode rc = jsonstats_init();
    if (rc != JSONUTIL_SUCCESS) {
        ValkeyModule_Log(ctx, "warning", "%s", jsonutil_code_to_message(rc));
        return VALKEYMODULE_ERR;
    }

    // Register info callback
    if (ValkeyModule_RegisterInfoFunc(ctx, Module_Info) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to register module info callback.");
        return VALKEYMODULE_ERR;
    }

    const char *cmdflg_readonly        = "fast readonly";
    const char *cmdflg_slow_write_deny = "write deny-oom";
    const char *cmdflg_fast_write      = "fast write";
    const char *cmdflg_fast_write_deny = "fast write deny-oom";
    const char *cmdflg_debug           = "readonly getkeys-api";
    char json_category[] = "json";

    if (ValkeyModule_AddACLCategory(ctx, json_category) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    const char *cat_readonly        = "json read fast";
    const char *cat_slow_write_deny = "json write slow";
    const char *cat_fast_write      = "json write fast";
    const char *cat_fast_write_deny = "json write fast";
    const char *cat_debug           = "json read slow";

    // Register commands
    if (ValkeyModule_CreateCommand(ctx, "JSON.SET", Command_JsonSet, cmdflg_slow_write_deny, 1, 1, 1)
        == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.SET.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.SET"), cat_slow_write_deny) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.SET.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.GET", Command_JsonGet, cmdflg_readonly, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.GET.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.GET"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.GET.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.MGET", Command_JsonMGet, cmdflg_readonly, 1, -2, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.MGET.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.MGET"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.MGET.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.DEL", Command_JsonDel, cmdflg_fast_write, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.DEL.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.DEL"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.DEL.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.FORGET", Command_JsonDel, cmdflg_fast_write, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.FORGET.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.FORGET"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.FORGET.");
        return VALKEYMODULE_ERR;
    }
   
    if (ValkeyModule_CreateCommand(ctx, "JSON.NUMINCRBY", Command_JsonNumIncrBy,
                                  cmdflg_fast_write, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.NUMINCRBY.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.NUMINCRBY"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.NUMINCRBY.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.NUMMULTBY", Command_JsonNumMultBy,
                                  cmdflg_fast_write, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.NUMMULTBY.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.NUMMULTBY"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.NUMMULTBY.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.STRLEN", Command_JsonStrLen, cmdflg_readonly, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.STRLEN.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.STRLEN"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.STRLEN.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.STRAPPEND", Command_JsonStrAppend,
                                  cmdflg_fast_write_deny, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.STRAPPEND.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.STRAPPEND"), cat_fast_write_deny) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.STRAPPEND.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.TOGGLE", Command_JsonToggle,
                                  cmdflg_fast_write_deny, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.TOGGLE.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.TOGGLE"), cat_fast_write_deny) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.TOGGLE.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.OBJLEN", Command_JsonObjLen, cmdflg_readonly, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.OBJLEN.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.OBJLEN"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.OBJLEN.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.OBJKEYS", Command_JsonObjKeys, cmdflg_readonly, 1, 1, 1) ==
        VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.OBJKEYS.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.OBJKEYS"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.OBJKEYS.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.ARRLEN", Command_JsonArrLen, cmdflg_readonly, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.ARRLEN.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.ARRLEN"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.ARRLEN.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.ARRAPPEND", Command_JsonArrAppend,
                                  cmdflg_fast_write_deny, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.ARRAPPEND.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.ARRAPPEND"), cat_fast_write_deny) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.ARRAPPEND.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateCommand(ctx, "JSON.ARRPOP", Command_JsonArrPop, cmdflg_fast_write, 1, 1, 1) ==
        VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.ARRPOP.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.ARRPOP"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.ARRPOP.");
        return VALKEYMODULE_ERR;
    }


    if (ValkeyModule_CreateCommand(ctx, "JSON.ARRINSERT", Command_JsonArrInsert,
                                  cmdflg_fast_write_deny, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.ARRINSERT.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.ARRINSERT"), cat_fast_write_deny) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.ARRINSERT.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.ARRTRIM", Command_JsonArrTrim, cmdflg_fast_write,
                                  1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.ARRTRIM.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.ARRTRIM"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.ARRTRIM.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.CLEAR", Command_JsonClear, cmdflg_fast_write,
                                  1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.CLEAR.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.CLEAR"), cat_fast_write) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.CLEAR.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.ARRINDEX", Command_JsonArrIndex, cmdflg_readonly,
                                  1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.ARRINDEX.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.ARRINDEX"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.ARRINDEX.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.TYPE", Command_JsonType, cmdflg_readonly, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.TYPE.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.TYPE"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.TYPE.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.RESP", Command_JsonResp, cmdflg_readonly, 1, 1, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.RESP.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.RESP"), cat_readonly) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.RESP.");
        return VALKEYMODULE_ERR;
    }

    if (ValkeyModule_CreateCommand(ctx, "JSON.DEBUG", NULL, cmdflg_debug, 0, 0, 0) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_SetCommandACLCategories(ValkeyModule_GetCommand(ctx,"JSON.DEBUG"), cat_debug) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to set command category for JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }


    // Register JSON.DEBUG subcommands
    ValkeyModuleCommand *parent = ValkeyModule_GetCommand(ctx, "JSON.DEBUG");
    if (ValkeyModule_CreateSubcommand(parent, "MEMORY", Command_JsonDebug, "", 2, 2, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand MEMORY for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "DEPTH", Command_JsonDebug, "", 2, 2, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand DEPTH for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "FIELDS", Command_JsonDebug, "", 2, 2, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand FIELDS for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "HELP", Command_JsonDebug, "", 0, 0, 0) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand HELP for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "MAX-DEPTH-KEY", Command_JsonDebug, "", 0, 0, 0) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand MAX-DEPTH-KEY for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "MAX-SIZE-KEY", Command_JsonDebug, "", 0, 0, 0) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand MAX-SIZE-KEY for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "KEYTABLE-CHECK", Command_JsonDebug, "", 0, 0, 0) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand KEYTABLE-CHECK for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "KEYTABLE-CORRUPT", Command_JsonDebug, "", 2, 2, 1) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand KEYTABLE-CORRUPT for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateSubcommand(parent, "KEYTABLE-DISTRIBUTION", Command_JsonDebug, "", 0, 0, 0)
        == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to create subcommand KEYTABLE-DISTRIBUTION for command JSON.DEBUG.");
        return VALKEYMODULE_ERR;
    }

    // key-spec flags categories
    const uint64_t ks_read_write_update = VALKEYMODULE_CMD_KEY_RW | VALKEYMODULE_CMD_KEY_UPDATE;
    const uint64_t ks_read_write_insert = VALKEYMODULE_CMD_KEY_RW | VALKEYMODULE_CMD_KEY_INSERT;
    const uint64_t ks_read_write_delete = VALKEYMODULE_CMD_KEY_RW | VALKEYMODULE_CMD_KEY_DELETE;

    const uint64_t ks_read_write_access_update = ks_read_write_update | VALKEYMODULE_CMD_KEY_ACCESS;
    const uint64_t ks_read_write_access_delete = ks_read_write_delete | VALKEYMODULE_CMD_KEY_ACCESS;

    const uint64_t ks_read_only = VALKEYMODULE_CMD_KEY_RO;
    const uint64_t ks_read_only_access = VALKEYMODULE_CMD_KEY_RO | VALKEYMODULE_CMD_KEY_ACCESS;

    // Commands under RW + Update
    if (!set_command_info(ctx, "JSON.SET", -4, ks_read_write_update, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    // Commands under RW + Insert
    if (!set_command_info(ctx, "JSON.ARRAPPEND", -4, ks_read_write_insert, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.STRAPPEND", -3, ks_read_write_insert, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.ARRINSERT", -5, ks_read_write_insert, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    // Commands under RW + Delete
    if (!set_command_info(ctx, "JSON.DEL", -2, ks_read_write_delete, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.FORGET", -2, ks_read_write_delete, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.ARRTRIM", 5, ks_read_write_delete, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.CLEAR", -2, ks_read_write_delete, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    // Commands under RW + Access + Update
    if (!set_command_info(ctx, "JSON.NUMINCRBY", 4, ks_read_write_access_update, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.NUMMULTBY", 4, ks_read_write_access_update, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.TOGGLE", -2, ks_read_write_access_update, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    // Commands under RW + Access + Delete
    if (!set_command_info(ctx, "JSON.ARRPOP", -2, ks_read_write_access_delete, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }

    // Commands under RO + Access
    if (!set_command_info(ctx, "JSON.GET", -2, ks_read_only_access, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.MGET", -3, ks_read_only_access, 1, std::make_tuple(-2, 1, 0))) {
         return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.OBJKEYS", -2, ks_read_only_access, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.ARRINDEX", -4, ks_read_only_access, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.TYPE", -2, ks_read_only_access, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.RESP", -2, ks_read_only_access, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }

    // Commands under RO
    if (!set_command_info(ctx, "JSON.STRLEN", -2, ks_read_only, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.OBJLEN", -2, ks_read_only, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.ARRLEN", -2, ks_read_only, 1, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }

    // JSON.DEBUG and its sub-commands
    if (!set_command_info(ctx, "JSON.DEBUG", -2)) return VALKEYMODULE_ERR;
    if (!set_command_info(ctx, "JSON.DEBUG|MEMORY", -3, ks_read_only_access, 2, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.DEBUG|FIELDS", -3, ks_read_only_access, 2, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.DEBUG|DEPTH", 3, ks_read_only_access, 2, std::make_tuple(0, 1, 0))) {
        return VALKEYMODULE_ERR;
    }
    if (!set_command_info(ctx, "JSON.DEBUG|HELP", 2)) return VALKEYMODULE_ERR;
    // admin commands
    if (!set_command_info(ctx, "JSON.DEBUG|MAX-DEPTH-KEY", 2)) return VALKEYMODULE_ERR;
    if (!set_command_info(ctx, "JSON.DEBUG|MAX-SIZE-KEY", 2)) return VALKEYMODULE_ERR;
    if (!set_command_info(ctx, "JSON.DEBUG|KEYTABLE-CHECK", 2)) return VALKEYMODULE_ERR;
    if (!set_command_info(ctx, "JSON.DEBUG|KEYTABLE-CORRUPT", 3)) return VALKEYMODULE_ERR;
    if (!set_command_info(ctx, "JSON.DEBUG|KEYTABLE-DISTRIBUTION", 3)) return VALKEYMODULE_ERR;

    if (!memory_traps_control(false)) {
        ValkeyModule_Log(ctx, "warning", "Failed to setup memory trap control");
        return VALKEYMODULE_ERR;
    }

    //
    // Setup the global string table
    //
    initKeyTable(KeyTable::MAX_SHARDS, KeyTable::Factors());
    if (registerModuleConfigs(ctx) == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
