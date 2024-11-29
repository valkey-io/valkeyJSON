#include "json/util.h"
#include "json/dom.h"
#include "json/alloc.h"
#include <cstring>
#include <string>
#include "json/rapidjson_includes.h"

const char *jsonutil_code_to_message(JsonUtilCode code) {
    switch (code) {
        case JSONUTIL_SUCCESS:
        case JSONUTIL_WRONG_NUM_ARGS:
        case JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED:
            // only used as code, no message needed
            break;
        case JSONUTIL_JSON_PARSE_ERROR: return "SYNTAXERR Failed to parse JSON string due to syntax error";
        case JSONUTIL_NX_XX_SHOULD_BE_MUTUALLY_EXCLUSIVE:
            return "SYNTAXERR Option NX and XX should be mutually exclusive";
        case JSONUTIL_INVALID_JSON_PATH: return "SYNTAXERR Invalid JSON path";
        case JSONUTIL_INVALID_MEMBER_NAME: return "SYNTAXERR Invalid object member name";
        case JSONUTIL_INVALID_NUMBER: return "SYNTAXERR Invalid number";
        case JSONUTIL_INVALID_IDENTIFIER: return "SYNTAXERR Invalid identifier";
        case JSONUTIL_INVALID_DOT_SEQUENCE: return "SYNTAXERR Invalid dot sequence";
        case JSONUTIL_EMPTY_EXPR_TOKEN: return "SYNTAXERR Expression token cannot be empty";
        case JSONUTIL_ARRAY_INDEX_NOT_NUMBER: return "SYNTAXERR Array index is not a number";
        case JSONUTIL_STEP_CANNOT_NOT_BE_ZERO: return "SYNTAXERR Step in the slice cannot be zero";
        case JSONUTIL_INVALID_USE_OF_WILDCARD: return "ERR Invalid use of wildcard";
        case JSONUTIL_JSON_PATH_NOT_EXIST: return "NONEXISTENT JSON path does not exist";
        case JSONUTIL_PARENT_ELEMENT_NOT_EXIST: return "NONEXISTENT Parent element does not exist";
        case JSONUTIL_DOCUMENT_KEY_NOT_FOUND: return "NONEXISTENT Document key does not exist";
        case JSONUTIL_NOT_A_DOCUMENT_KEY: return "WRONGTYPE Not a JSON document key";
        case JSONUTIL_FAILED_TO_DELETE_VALUE: return "OPFAIL Failed to delete JSON value";
        case JSONUTIL_JSON_ELEMENT_NOT_NUMBER: return "WRONGTYPE JSON element is not a number";
        case JSONUTIL_JSON_ELEMENT_NOT_BOOL: return "WRONGTYPE JSON element is not a bool";
        case JSONUTIL_JSON_ELEMENT_NOT_STRING: return "WRONGTYPE JSON element is not a string";
        case JSONUTIL_JSON_ELEMENT_NOT_OBJECT: return "WRONGTYPE JSON element is not an object";
        case JSONUTIL_JSON_ELEMENT_NOT_ARRAY: return "WRONGTYPE JSON element is not an array";
        case JSONUTIL_VALUE_NOT_NUMBER: return "WRONGTYPE Value is not a number";
        case JSONUTIL_VALUE_NOT_STRING: return "WRONGTYPE Value is not a string";
        case JSONUTIL_VALUE_NOT_INTEGER: return "WRONGTYPE Value is not an integer";
        case JSONUTIL_PATH_SHOULD_BE_AT_THE_END: return "SYNTAXERR Path arguments should be positioned at the end";
        case JSONUTIL_COMMAND_SYNTAX_ERROR: return "SYNTAXERR Command syntax error";
        case JSONUTIL_MULTIPLICATION_OVERFLOW: return "OVERFLOW Multiplication would overflow";
        case JSONUTIL_ADDITION_OVERFLOW: return "OVERFLOW Addition would overflow";
        case JSONUTIL_EMPTY_JSON_OBJECT: return "EMPTYVAL Empty JSON object";
        case JSONUTIL_EMPTY_JSON_ARRAY: return "EMPTYVAL Empty JSON array";
        case JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES: return "OUTOFBOUNDARIES Array index is out of bounds";
        case JSONUTIL_UNKNOWN_SUBCOMMAND: return "SYNTAXERR Unknown subcommand";
        case JSONUTIL_FAILED_TO_CREATE_THREAD_SPECIFIC_DATA_KEY:
            return "PTHREADERR Failed to create thread-specific data key";
        case JSONUTIL_DOCUMENT_SIZE_LIMIT_EXCEEDED:
            return "LIMIT Document size limit is exceeded";
        case JSONUTIL_DOCUMENT_PATH_LIMIT_EXCEEDED:
            return "LIMIT Document path nesting limit is exceeded";
        case JSONUTIL_PARSER_RECURSION_DEPTH_LIMIT_EXCEEDED:
            return "LIMIT Parser recursion depth is exceeded";
        case JSONUTIL_RECURSIVE_DESCENT_TOKEN_LIMIT_EXCEEDED:
            return "LIMIT Total number of recursive descent tokens in the query string exceeds the limit";
        case JSONUTIL_QUERY_STRING_SIZE_LIMIT_EXCEEDED:
            return "LIMIT Query string size limit is exceeded";
        case JSONUTIL_CANNOT_INSERT_MEMBER_INTO_NON_OBJECT_VALUE:
            return "ERROR Cannot insert a member into a non-object value";
        case JSONUTIL_INVALID_RDB_FORMAT:
            return "ERROR Invalid value in RDB format";
        case JSONUTIL_DOLLAR_CANNOT_APPLY_TO_NON_ROOT: return "SYNTAXERR Dollar sign cannot apply to non-root element";
        default: ValkeyModule_Assert(false);
    }
    return "";
}

size_t jsonutil_double_to_string(const double val, char *double_to_string_buf, size_t len) {
    // It's safe to write a double value into double_to_string_buf, because the converted string will
    // never exceed length of 1024.
    ValkeyModule_Assert(len == BUF_SIZE_DOUBLE_JSON);
    return snprintf(double_to_string_buf, len, "%.17g", val);
}

/**
 * Convert double to string using the same format as RapidJSON's Writer::WriteDouble does.
 */
size_t jsonutil_double_to_string_rapidjson(const double val, char *double_to_string_buf_rapidjson, size_t len) {
    // RapidJSON's Writer::WriteDouble only uses a buffer of 25 bytes.
    ValkeyModule_Assert(len == BUF_SIZE_DOUBLE_RAPID_JSON);
    char *end = rapidjson::internal::dtoa(val, double_to_string_buf_rapidjson,
                                          rapidjson::Writer<rapidjson::StringBuffer>::kDefaultMaxDecimalPlaces);
    *end = '\0';
    return end - double_to_string_buf_rapidjson;
}

bool jsonutil_is_int64(const double a) {
    int64_t a_l = static_cast<long long>(a);
    double b = static_cast<double>(a_l);
    return (a <= b && a >= b);
}

JsonUtilCode jsonutil_multiply_double(const double a, const double b, double *res) {
    double c = a * b;
    // check overflow
    if (std::isinf(c)) return JSONUTIL_MULTIPLICATION_OVERFLOW;
    *res = c;
    return JSONUTIL_SUCCESS;
}

JsonUtilCode jsonutil_multiply_int64(const int64_t a, const int64_t b, int64_t *res) {
    if (a == 0 || b == 0) {
        *res = 0;
        return JSONUTIL_SUCCESS;
    }
    // Check overflow conditions without performing multiplication
    if ((a > 0 && b > 0 && a > INT64_MAX / b) ||    // Positive * Positive overflow
        (a > 0 && b < 0 && b < INT64_MIN / a) ||    // Positive * Negative overflow
        (a < 0 && b > 0 && a < INT64_MIN / b) ||    // Negative * Positive overflow
        (a < 0 && b < 0 && a < INT64_MAX / b)) {    // Negative * Negative overflow
        return JSONUTIL_MULTIPLICATION_OVERFLOW;
    }

    // If no overflow, perform the multiplication
    *res = a * b;
    return JSONUTIL_SUCCESS;
    return JSONUTIL_SUCCESS;
}

JsonUtilCode jsonutil_add_double(const double a, const double b, double *res) {
    double c = a + b;
    // check overflow
    if (std::isinf(c)) return JSONUTIL_ADDITION_OVERFLOW;
    *res = c;
    return JSONUTIL_SUCCESS;
}

JsonUtilCode jsonutil_add_int64(const int64_t a, const int64_t b, int64_t *res) {
    if (a >= 0) {
        if (b > INT64_MAX - a) return JSONUTIL_ADDITION_OVERFLOW;
    } else {
        if (b < INT64_MIN - a) return JSONUTIL_ADDITION_OVERFLOW;
    }
    *res = a + b;
    return JSONUTIL_SUCCESS;
}

bool jsonutil_is_root_path(const char *json_path) {
    return !strcmp(json_path, ".") || !strcmp(json_path, "$");
}
