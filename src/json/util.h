/**
 * This is the utility module, containing shared utility and helper code.
 *
 * Coding Conventions & Best Practices:
 * 1. Every public interface method declared in this file should be prefixed with "jsonutil_".
 * 2. Generally speaking, interface methods should not have Valkey module types such as ValkeyModuleCtx
 *    or ValkeyModuleString, because that would make unit tests hard to write, unless gmock classes
 *    have been developed.
 */
#ifndef VALKEYJSONMODULE_JSON_UTIL_H_
#define VALKEYJSONMODULE_JSON_UTIL_H_

#include <cmath>

extern "C" {
#define VALKEYMODULE_EXPERIMENTAL_API
#include <./include/valkeymodule.h>
}

typedef enum {
    JSONUTIL_SUCCESS = 0,
    JSONUTIL_WRONG_NUM_ARGS,
    JSONUTIL_JSON_PARSE_ERROR,
    JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED,
    JSONUTIL_NX_XX_SHOULD_BE_MUTUALLY_EXCLUSIVE,
    JSONUTIL_INVALID_JSON_PATH,
    JSONUTIL_INVALID_USE_OF_WILDCARD,
    JSONUTIL_INVALID_MEMBER_NAME,
    JSONUTIL_INVALID_NUMBER,
    JSONUTIL_INVALID_IDENTIFIER,
    JSONUTIL_INVALID_DOT_SEQUENCE,
    JSONUTIL_EMPTY_EXPR_TOKEN,
    JSONUTIL_ARRAY_INDEX_NOT_NUMBER,
    JSONUTIL_STEP_CANNOT_NOT_BE_ZERO,
    JSONUTIL_JSON_PATH_NOT_EXIST,
    JSONUTIL_PARENT_ELEMENT_NOT_EXIST,
    JSONUTIL_DOCUMENT_KEY_NOT_FOUND,
    JSONUTIL_NOT_A_DOCUMENT_KEY,
    JSONUTIL_FAILED_TO_DELETE_VALUE,
    JSONUTIL_JSON_ELEMENT_NOT_NUMBER,
    JSONUTIL_JSON_ELEMENT_NOT_BOOL,
    JSONUTIL_JSON_ELEMENT_NOT_STRING,
    JSONUTIL_JSON_ELEMENT_NOT_OBJECT,
    JSONUTIL_JSON_ELEMENT_NOT_ARRAY,
    JSONUTIL_VALUE_NOT_NUMBER,
    JSONUTIL_VALUE_NOT_STRING,
    JSONUTIL_VALUE_NOT_INTEGER,
    JSONUTIL_PATH_SHOULD_BE_AT_THE_END,
    JSONUTIL_COMMAND_SYNTAX_ERROR,
    JSONUTIL_MULTIPLICATION_OVERFLOW,
    JSONUTIL_ADDITION_OVERFLOW,
    JSONUTIL_EMPTY_JSON_OBJECT,
    JSONUTIL_EMPTY_JSON_ARRAY,
    JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES,
    JSONUTIL_UNKNOWN_SUBCOMMAND,
    JSONUTIL_FAILED_TO_CREATE_THREAD_SPECIFIC_DATA_KEY,
    JSONUTIL_DOCUMENT_SIZE_LIMIT_EXCEEDED,
    JSONUTIL_DOCUMENT_PATH_LIMIT_EXCEEDED,
    JSONUTIL_PARSER_RECURSION_DEPTH_LIMIT_EXCEEDED,
    JSONUTIL_RECURSIVE_DESCENT_TOKEN_LIMIT_EXCEEDED,
    JSONUTIL_QUERY_STRING_SIZE_LIMIT_EXCEEDED,
    JSONUTIL_CANNOT_INSERT_MEMBER_INTO_NON_OBJECT_VALUE,
    JSONUTIL_INVALID_RDB_FORMAT,
    JSONUTIL_DOLLAR_CANNOT_APPLY_TO_NON_ROOT,
    JSONUTIL_LAST
} JsonUtilCode;

typedef struct {
    const char *newline;
    const char *space;
    const char *indent;
} PrintFormat;

/* Enums for buffer sizes used in conversion of double to json or double to rapidjson */
enum { BUF_SIZE_DOUBLE_JSON = 32, BUF_SIZE_DOUBLE_RAPID_JSON = 25};

/* Get message for a given code. */
const char *jsonutil_code_to_message(JsonUtilCode code);

/* Convert a double value to string. This method is used to help serializing numbers to strings.
 * Trailing zeros will be removed. For example, 135.250000 will be converted to string 135.25.
 */
size_t jsonutil_double_to_string(const double val, char *double_to_string_buf, size_t len);

/**
 * Convert double to string using the same format as RapidJSON's Writer::WriteDouble does.
 */
size_t jsonutil_double_to_string_rapidjson(const double val, char* double_to_string_buf_rapidjson, size_t len);

/* Check if a double value is int64.
 * If the given double does not equal an integer (int64), return false.
 * If the given double is out of range of int64, return false.
 */
bool jsonutil_is_int64(const double a);

/* Multiple two double numbers with overflow check.
 * @param res - OUTPUT parameter, *res stores the result of multiplication.
 * @return JSONUTIL_SUCCESS if successful, JSONUTIL_MULTIPLICATION_OVERFLOW if the result overflows.
 */
JsonUtilCode jsonutil_multiply_double(const double a, const double b, double *res);

/* Multiple two int64 numbers with overflow check.
 * @param res - OUTPUT parameter, *res stores the result of multiplication.
 * @return JSONUTIL_SUCCESS if successful, JSONUTIL_MULTIPLICATION_OVERFLOW if the result overflows.
 */
JsonUtilCode jsonutil_multiply_int64(const int64_t a, const int64_t b, int64_t *res);

/* Add two double numbers with overflow check.
 * @param res - OUTPUT parameter, *res stores the result of addition.
 * @return JSONUTIL_SUCCESS if successful, JSONUTIL_ADDITION_OVERFLOW if the result overflows.
 */
JsonUtilCode jsonutil_add_double(const double a, const double b, double *res);

/* Add two int64 numbers with overflow check.
 * @param res - OUTPUT parameter, *res stores the result of addition.
 * @return JSONUTIL_SUCCESS if successful, JSONUTIL_ADDITION_OVERFLOW if the result overflows.
 */
JsonUtilCode jsonutil_add_int64(const int64_t a, const int64_t b, int64_t *res);

bool jsonutil_is_root_path(const char *json_path);

#endif  // VALKEYJSONMODULE_JSON_UTIL_H_
