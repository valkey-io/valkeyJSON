#include "json/dom.h"
#include "json/json.h"
#include "json/stats.h"
#include "json/selector.h"
#include <cstring>
#include <memory>
#include <iostream>
#include <iomanip>
#include <cmath>
#include "json/rapidjson_includes.h"

#define STATIC /* decorator for static functions, remove so that backtrace symbols include these */

#define CHECK_DOCUMENT_SIZE_LIMIT(ctx, curr_doc_size, input_json_val_size) \
{ \
    if (ctx != nullptr && !(ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_REPLICATED) && \
        json_get_max_document_size() > 0 && (curr_doc_size + input_json_val_size > json_get_max_document_size())) { \
         ValkeyModule_Log(ctx, "debug", \
         "Document size limit is exceeded. The attempted operation will result in a document with %lu bytes of " \
         "memory size.", curr_doc_size + input_json_val_size); \
        return JSONUTIL_DOCUMENT_SIZE_LIMIT_EXCEEDED; \
    } \
}

#define CHECK_DOCUMENT_PATH_LIMIT(ctx, selector, new_val) \
{ \
    size_t __depth_would_be = selector.getMaxPathDepth() + new_val.GetMaxDepth(); \
    if (__depth_would_be > json_get_max_path_limit()) { \
         ValkeyModule_Log(ctx, "debug", \
         "Document path limit is exceeded. The attempted operation will result in a document with %lu nesting" \
         " levels.", __depth_would_be); \
        return JSONUTIL_DOCUMENT_PATH_LIMIT_EXCEEDED; \
    } else { \
        jsonstats_update_max_depth_ever_seen(__depth_would_be); \
    } \
}

// the one true allocator
RapidJsonAllocator allocator;

/**
 * We want to avoid all redundant creations of an allocator -- for performance reasons.
 * So we use the constructor to detect that situation. It's free after startup. If you trip
 * this trap, then you let a rapidjson allocator instance get defaulted to 0 somewhere in your code.
 */
RapidJsonAllocator::RapidJsonAllocator() {
    ValkeyModule_Assert(this == &allocator);  // Only this one is allowed :)
}

JValue& dom_get_value(JDocument &doc) {
    return doc.GetJValue();
}

JParser& JParser::Parse(const char *json, size_t len) {
    int64_t begin_val = jsonstats_begin_track_mem();
    RJParser::Parse(json, len);
    int64_t delta = jsonstats_end_track_mem(begin_val);
    ValkeyModule_Assert(delta >= 0);
    allocated_size = static_cast<size_t>(delta);
    return *this;
}

JParser& JParser::Parse(const std::string_view &sv) {
    return Parse(sv.data(), sv.length());
}

jsn::string validate(const JDocument *doc) {
    std::string s = doc->GetJValue().Validate();
    return jsn::string(s.c_str(), s.length());
}

STATIC JDocument *create_doc() {
    return new JDocument();
}

void dom_free_doc(JDocument *doc) {
    ValkeyModule_Assert(doc != nullptr);
    delete doc;
}

size_t dom_get_doc_size(const JDocument *doc) {
    return doc->size;
}

void dom_set_doc_size(JDocument *doc, const size_t size) {
    doc->size = size;
}

size_t dom_get_bucket_id(const JDocument *doc) {
    return doc->bucket_id;
}

void dom_set_bucket_id(JDocument *doc, const uint32_t bucket_id) {
    doc->bucket_id = bucket_id;
}

JsonUtilCode dom_parse(ValkeyModuleCtx *ctx, const char *json_buf, const size_t buf_len, JDocument **doc) {
    *doc = nullptr;
    JParser parser;
    if (parser.Parse(json_buf, buf_len).HasParseError()) {
        return parser.GetParseErrorCode();
    }
    CHECK_DOCUMENT_SIZE_LIMIT(ctx, size_t(0), parser.GetJValueSize())
    *doc = create_doc();
    (*doc)->SetJValue(parser.GetJValue());
    jsonstats_update_max_depth_ever_seen(parser.GetMaxDepth());
    return JSONUTIL_SUCCESS;
}

STATIC bool has_custom_format(const PrintFormat *format) {
    return (format != nullptr && (format->indent != nullptr || format->space != nullptr || format->newline != nullptr));
}
/**
 * Serialize a value.
 * @param json OUTPUT param, serialized string is appended to the param.
 */
STATIC void serialize_value(const JValue &val, size_t initialLevel, const PrintFormat *format,
                            rapidjson::StringBuffer &oss) {
    size_t max_depth = 0;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(oss);
    if (has_custom_format(format)) {
        if (format && format->newline) writer.SetNewline(std::string_view(format->newline, strlen(format->newline)));
        if (format && format->indent) writer.SetIndent(std::string_view(format->indent, strlen(format->indent)));
        if (format && format->space) writer.SetSpace(std::string_view(format->space, strlen(format->space)));
        writer.SetInitialLevel(initialLevel);
        val.Accept(writer);
        jsonstats_update_max_depth_ever_seen(writer.GetMaxDepth());
    } else {
        writer.FastWrite(val, &max_depth);
        jsonstats_update_max_depth_ever_seen(max_depth);
    }
}

STATIC void serialize_value(const JValue &val, size_t initialLevel, const PrintFormat *format, ReplyBuffer& oss) {
    size_t max_depth = 0;
    rapidjson::PrettyWriter<ReplyBuffer> writer(oss);
    if (has_custom_format(format)) {
        if (format && format->newline) writer.SetNewline(std::string_view(format->newline, strlen(format->newline)));
        if (format && format->indent) writer.SetIndent(std::string_view(format->indent, strlen(format->indent)));
        if (format && format->space) writer.SetSpace(std::string_view(format->space, strlen(format->space)));
        writer.SetInitialLevel(initialLevel);
        val.Accept(writer);
        jsonstats_update_max_depth_ever_seen(writer.GetMaxDepth());
    } else {
        writer.FastWrite(val, &max_depth);
        jsonstats_update_max_depth_ever_seen(max_depth);
    }
}

void dom_serialize(JDocument *doc, const PrintFormat *format, rapidjson::StringBuffer &oss) {
    serialize_value(*(doc), 0, format, oss);
}

void dom_serialize_value(const JValue &val, const PrintFormat *format, rapidjson::StringBuffer &oss) {
    serialize_value(val, 0, format, oss);
}

JsonUtilCode dom_set_value(ValkeyModuleCtx *ctx, JDocument *doc, const char *json_path, const char *new_val_json,
                           size_t new_val_size, const bool is_create_only, const bool is_update_only) {
    if (is_create_only && is_update_only) return JSONUTIL_NX_XX_SHOULD_BE_MUTUALLY_EXCLUSIVE;

    Selector selector;
    JsonUtilCode rc = selector.prepareSetValues(doc->GetJValue(), json_path);
    if (rc != JSONUTIL_SUCCESS) return rc;

    if (is_create_only && selector.hasUpdates()) return JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED;
    if (is_update_only && selector.hasInserts()) return JSONUTIL_NX_XX_CONDITION_NOT_SATISFIED;

    JParser new_val;
    if (new_val.Parse(new_val_json, new_val_size).HasParseError()) {
        return new_val.GetParseErrorCode();
    }

    CHECK_DOCUMENT_PATH_LIMIT(ctx, selector, new_val)
    CHECK_DOCUMENT_SIZE_LIMIT(ctx, doc->size, new_val.GetJValueSize())

    selector.commit(new_val);
    return JSONUTIL_SUCCESS;
}

template<typename OutputBuffer>
STATIC void PutString(OutputBuffer& oss, const char *str) {
    while (*str) oss.Put(*str++);
}

template<typename OutputBuffer>
STATIC void PutEscapedString(OutputBuffer& oss, const char *str) {
    JValue tmp;
    tmp.SetString(str, strlen(str));
    serialize_value(tmp, 0, nullptr, oss);
}

// Build stringified JSON array directly from a vector of values.
template<typename T>
STATIC void build_json_array(const jsn::vector<JValue*> &values, const PrintFormat *format, T &oss) {
    bool has_format = has_custom_format(format);
    oss.Put('[');
    if (has_format && format->newline) PutString(oss, format->newline);
    for (size_t i=0; i < values.size(); i++) {
        if (has_format && format->indent) PutString(oss, format->indent);
        serialize_value(*values[i], 1, format, oss);
        if (i < values.size() - 1) oss.Put(',');
        if (has_format && format->newline) PutString(oss, format->newline);
    }
    oss.Put(']');
}

template STATIC void build_json_array(const jsn::vector<JValue*> &values, const PrintFormat *format, ReplyBuffer &oss);
template STATIC void build_json_array(const jsn::vector<JValue*> &values, const PrintFormat *format,
        rapidjson::StringBuffer &oss);

template<typename T>
JsonUtilCode dom_get_value_as_str(JDocument *doc, const char *json_path, const PrintFormat *format,
                                  T &oss, const bool update_stats) {
    Selector selector;
    JsonUtilCode rc = selector.getValues(*doc, json_path);
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // If legacy path, return either the first value, or NONEXISTENT error if no value is found.
    if (selector.isLegacyJsonPathSyntax()) {
        if (values.empty()) {
            return JSONUTIL_JSON_PATH_NOT_EXIST;
        } else {
            serialize_value(*values[0], 0, format, oss);
            // update stats
            if (update_stats) jsonstats_update_stats_on_read(oss.GetLength());
            return JSONUTIL_SUCCESS;
        }
    }

    // v2 path: return an array of values.
    if (values.empty()) {
        // return an empty array
        oss.Put('[');
        oss.Put(']');
    } else {
        // Multiple values are returned to the client as a JSON array.
        build_json_array(values, format, oss);
    }

    // update stats
    if (update_stats) jsonstats_update_stats_on_read(oss.GetLength());
    return JSONUTIL_SUCCESS;
}

template JsonUtilCode dom_get_value_as_str(JDocument *doc, const char *json_path, const PrintFormat *format,
                                           ReplyBuffer &oss, const bool update_stats);
template JsonUtilCode dom_get_value_as_str(JDocument *doc, const char *json_path, const PrintFormat *format,
                                           rapidjson::StringBuffer &oss, const bool update_stats);

STATIC void appendPathAndValue(const char *key, const JValue &val, const bool isLastPath,
                               const bool has_format, const PrintFormat *format, ReplyBuffer &oss) {
    if (has_format && format->indent) PutString(oss, format->indent);
    PutEscapedString(oss, key);
    oss.Put(':');
    if (has_format && format->space) PutString(oss, format->space);
    serialize_value(val, 1, format, oss);
    if (!isLastPath) oss.Put(',');
    if (has_format && format->newline) PutString(oss, format->newline);
}

STATIC void appendPathAndValues(const char *key, const jsn::vector<JValue*> &values, const bool isLastPath,
                                const bool has_format, const PrintFormat *format, ReplyBuffer &oss) {
    if (has_format && format->indent) PutString(oss, format->indent);
    PutEscapedString(oss, key);
    oss.Put(':');
    if (has_format && format->space) PutString(oss, format->space);
    oss.Put('[');
    if (has_format && format->newline) PutString(oss, format->newline);

    for (size_t i=0; i < values.size(); i++) {
        if (has_format && format->indent) {
            PutString(oss, format->indent);
            PutString(oss, format->indent);
        }
        serialize_value(*values[i], 2, format, oss);
        if (i < values.size() - 1) oss.Put(',');
        if (has_format && format->newline) PutString(oss, format->newline);
    }

    if (has_format && format->indent) PutString(oss, format->indent);
    oss.Put(']');
    if (!isLastPath) oss.Put(',');
    if (has_format && format->newline) PutString(oss, format->newline);
}

STATIC JsonUtilCode buildJsonForMultiPaths(JDocument *doc, const char **paths, const int num_paths,
                                           const bool is_v2path, const PrintFormat *format,
                                           ReplyBuffer &oss) {
    bool has_format = has_custom_format(format);
    Selector selector(is_v2path);
    JsonUtilCode rc;
    oss.Put('{');
    if (has_format && format->newline) PutString(oss, format->newline);
    for (int i = 0; i < num_paths; i++) {
        rc = selector.getValues(*doc, paths[i]);
        if (rc != JSONUTIL_SUCCESS) {
            if (!is_v2path) return rc;
            // For v2 path, return error code only if it's a syntax error.
            if (selector.isSyntaxError(rc)) return rc;
        }

        jsn::vector<JValue*> values;
        selector.getSelectedValues(values);

        if (!is_v2path) {  // legacy path
            if (values.empty()) {
                return JSONUTIL_JSON_PATH_NOT_EXIST;
            } else {
                appendPathAndValue(paths[i], *values[0], (i == num_paths - 1), has_format, format, oss);
            }
        } else {
            appendPathAndValues(paths[i], values, (i == num_paths - 1), has_format, format, oss);
        }
    }
    oss.Put('}');
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_get_values_as_str(JDocument *doc, const char **paths, const int num_paths,
                                   PrintFormat *format, ReplyBuffer &oss, const bool update_stats) {
    // If there are multiple paths mixed with both v1 and v2 syntax, the returned value should conform to the V2
    // behavior (returning an array of values).
    // We can't start processing the first element until we know if we should conform to V1 or V2 behavior.
    // Example:
    //   cmd1: json.get wikipedia .foo .address
    //   cmd2:  json.get wikipedia .foo $.address
    // The expected behavior is: Cmd1 should fail because .foo does not exist, while cmd2 should succeed because
    // overall the command should conform to V2 behavior (as the 2nd path is V2 path). Cmd2 should return the
    // following result:
    //   127.0.0.1:6379> json.get wikipedia .foo $.address
    //   {"$.address":[{"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"}],".foo":[]}
    //
    // Without the pre-knowledge of V1 vs V2, both commands would fail, because when the selector first runs ".foo",
    // it would think it is V1 and returns an error. The loop below would then exit without attempting the 2nd path.
    bool is_v2path = Selector::has_at_least_one_v2path(paths, num_paths);

    // Values at multiple paths are combined to form a serialized JSON object string, in which each path is a key.
    JsonUtilCode rc = buildJsonForMultiPaths(doc, paths, num_paths, is_v2path, format, oss);
    if (rc != JSONUTIL_SUCCESS) return rc;

    // update stats
    if (update_stats) jsonstats_update_stats_on_read(oss.GetLength());
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_delete_value(JDocument *doc, const char *json_path, size_t &num_vals_deleted) {
    Selector selector;
    return selector.deleteValues(doc->GetJValue(), json_path, num_vals_deleted);
}

// check if there is at least one number value
STATIC bool has_number_value(jsn::vector<JValue*> &values) {
    for (auto &v : values) {
        if (v->IsNumber()) return true;
    }
    return false;
}

JsonUtilCode dom_increment_by(JDocument *doc, const char *json_path, const JValue *incr_by,
                              jsn::vector<double> &out_vals, bool &is_v2_path) {
    out_vals.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), json_path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no number value is selected
        if (!has_number_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_NUMBER;
    }

    for (auto &val : selector.getUniqueResultSet()) {
        if (val.first->IsNumber()) {
            if (val.first->IsInt64() && incr_by->IsInt64()) {
                // All are integers
                int64_t res;
                rc = jsonutil_add_int64(val.first->GetInt64(), incr_by->GetInt64(), &res);
                if (rc == JSONUTIL_SUCCESS) {
                    val.first->SetInt64(res);
                    out_vals.push_back(res);
                    continue;
                }
            }

            double res;
            rc = jsonutil_add_double(val.first->GetDouble(), incr_by->GetDouble(), &res);
            if (rc != JSONUTIL_SUCCESS) return rc;
            char double_string[BUF_SIZE_DOUBLE_JSON];
            size_t len = jsonutil_double_to_string(res, double_string, sizeof(double_string));
            val.first->SetDouble(double_string, len, allocator);

            out_vals.push_back(res);
        } else {
            out_vals.push_back(std::nan("NaN"));  // indicates the value is not number
        }
    }

    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_multiply_by(JDocument *doc, const char *json_path, const JValue *mult_by,
                             jsn::vector<double> &out_vals, bool &is_v2_path) {
    out_vals.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), json_path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no number value is selected
        if (!has_number_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_NUMBER;
    }

    for (auto &val : selector.getUniqueResultSet()) {
        if (val.first->IsNumber()) {
            double res;
            rc = jsonutil_multiply_double(val.first->GetDouble(), mult_by->GetDouble(), &res);
            if (rc != JSONUTIL_SUCCESS) return rc;

            if (jsonutil_is_int64(res)) {
                val.first->SetInt64(static_cast<int64_t>(res));
            } else {
                char double_string[BUF_SIZE_DOUBLE_JSON];
                size_t len = jsonutil_double_to_string(res, double_string, sizeof(double_string));
                val.first->SetDouble(double_string, len, allocator);
            }

            out_vals.push_back(res);
        } else {
            out_vals.push_back(std::nan("NaN"));  // indicates the value is not number
        }
    }
    return JSONUTIL_SUCCESS;
}

// check if there is at least one boolean value
STATIC bool has_boolean_value(jsn::vector<JValue*> &values) {
    for (auto &v : values) {
        if (v->IsBool()) return true;
    }
    return false;
}

JsonUtilCode dom_toggle(JDocument *doc, const char *path, jsn::vector<int> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no boolean value is selected
        if (!has_boolean_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_BOOL;
    }

    for (auto &v : selector.getUniqueResultSet()) {
        if (v.first->IsBool()) {
            bool res = v.first->GetBool();
            res = !res;
            v.first->SetBool(res);
            vec.push_back(res? 1 : 0);
        } else {
            vec.push_back(-1);  // -1 means the source value is not boolean
        }
    }
    return JSONUTIL_SUCCESS;
}

// check if there is at least one string value
STATIC bool has_string_value(jsn::vector<JValue*> &values) {
    for (auto &v : values) {
        if (v->IsString()) return true;
    }
    return false;
}

JsonUtilCode dom_string_length(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no string value is selected
        if (!has_string_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_STRING;
    }

    for (auto &v : values) {
        if (v->IsString()) {
            vec.push_back(v->GetStringLength());
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-string value
        }
    }
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_string_append(JDocument *doc, const char *path, const char *json, const size_t json_len,
                               jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no string value is selected
        if (!has_string_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_STRING;
    }

    // verify the input json string is a valid
    JParser appendVal;
    if (appendVal.Parse(json, json_len).HasParseError()) return appendVal.GetParseErrorCode();
    if (!appendVal.GetJValue().IsString()) return JSONUTIL_VALUE_NOT_STRING;

    jsn::string str_append = jsn::string(appendVal.GetString());
    for (auto &v : selector.getUniqueResultSet()) {
        if (v.first->IsString()) {
            jsn::string new_string = jsn::string(v.first->GetString()) + str_append;
            v.first->SetString(new_string.c_str(), new_string.length(), allocator);
            vec.push_back(new_string.length());
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-string value
        }
    }
    return JSONUTIL_SUCCESS;
}

// check if there is at least one object value
STATIC bool has_object_value(jsn::vector<JValue*> &values) {
    for (auto &v : values) {
        if (v->IsObject()) return true;
    }
    return false;
}

JsonUtilCode dom_object_length(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no object value is selected
        if (!has_object_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_OBJECT;
    }

    for (auto &v : values) {
        if (v->IsObject()) {
            vec.push_back(v->MemberCount());
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-object value
        }
    }
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_object_keys(JDocument *doc, const char *path,
                             jsn::vector<jsn::vector<jsn::string>> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no object value is selected
        if (!has_object_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_OBJECT;
    }

    for (auto &v : values) {
        jsn::vector<jsn::string> keys;
        if (v->IsObject()) {
            for (auto &m : v->GetObject()) {
                keys.push_back(std::move(jsn::string(m.name.GetString(), m.name.GetStringLength())));
            }
        }
        vec.push_back(keys);
    }
    return JSONUTIL_SUCCESS;
}

// check if there is at least one array value
STATIC bool has_array_value(jsn::vector<JValue*> &values) {
    for (auto &v : values) {
        if (v->IsArray()) return true;
    }
    return false;
}

JsonUtilCode dom_array_length(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no array value is selected
        if (!has_array_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }

    for (auto &v : values) {
        if (v->IsArray()) {
            vec.push_back(v->Size());
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-array value
        }
    }
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_array_append(ValkeyModuleCtx *ctx, JDocument *doc, const char *path,
                              const char **jsons, size_t *json_lens, const size_t num_values,
                              jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no array value is selected
        if (!has_array_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }

    // parse json values
    jsn::vector<JParser> appendVals(num_values);
    size_t totalJValueSize = 0;
    for (size_t i=0; i < num_values; i++) {
        if (appendVals[i].Parse(jsons[i], json_lens[i]).HasParseError()) {
            return appendVals[i].GetParseErrorCode();
        }
        CHECK_DOCUMENT_PATH_LIMIT(ctx, selector, appendVals[i])
        totalJValueSize += appendVals[i].GetJValueSize();
    }
    CHECK_DOCUMENT_SIZE_LIMIT(ctx, doc->size, totalJValueSize)

    for (auto &v : selector.getUniqueResultSet()) {
        if (v.first->IsArray()) {
            for (size_t i=0; i < num_values; i++) {
                // Need to make a copy of the value because after the first call of JValue::PushBack,
                // the object is moved and can no longer be pushed into anther array.
                JValue copy(appendVals[i], allocator);
                v.first->PushBack(copy, allocator);
            }
            vec.push_back(v.first->Size());
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-array value
        }
    }
    return JSONUTIL_SUCCESS;
}

STATIC void internal_array_pop(JValue &arrVal, int64_t index, jsn::vector<rapidjson::StringBuffer> &vec,
                               rapidjson::StringBuffer &oss) {
    // Convert negative index to positive
    int64_t size = arrVal.Size();
    if (index < 0) index = (arrVal.Size() == 0 ? 0 : size + index);

    // Out-of-bound index is rounded to respective array bounds
    if (index >= size) index = size - 1;
    if (index < 0) index = 0;

    serialize_value(arrVal[index], 0, nullptr, oss);
    arrVal.Erase(arrVal.Begin() + index);
    vec.push_back(std::move(oss));
}

JsonUtilCode dom_array_pop(JDocument *doc, const char *path, int64_t index,
                           jsn::vector<rapidjson::StringBuffer> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no array value is selected
        if (!has_array_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }

    for (auto &v : selector.getUniqueResultSet()) {
        rapidjson::StringBuffer oss;
        if (v.first->IsArray()) {
            if (v.first->Empty()) {
                vec.push_back(std::move(oss));  // empty array, oss is empty
            } else {
                internal_array_pop(*v.first, index, vec, oss);
            }
        } else {
            vec.push_back(std::move(oss));  // non-array value, oss is empty
        }
    }

    return JSONUTIL_SUCCESS;
}

STATIC JsonUtilCode internal_array_insert(JValue &arrVal, jsn::vector<JParser> &insertVals,
                                          const size_t num_values, int64_t index, jsn::vector<size_t> &vec) {
    size_t size = arrVal.Size();

    // Negative index values are interpreted as starting from the end.
    if (index < 0) index = (arrVal.Size() == 0 ? 0 : size + index);

    // Return error if the index is out of bounds.
    // If index is size-1, we are inserting before the last element.
    // If index is size, we are appending to the array.
    if (index < 0 || index > static_cast<int64_t>(size)) return JSONUTIL_INDEX_OUT_OF_ARRAY_BOUNDARIES;

    // append num_values empty values
    for (size_t i=0; i < num_values; i++) {
        JValue empty;
        arrVal.PushBack(empty, allocator);
    }

    // shift values [index+1..end-num_values] to the right by num_values positions
    for (int64_t i = arrVal.Size() - 1; i >= static_cast<int64_t>(num_values) + index; i--) {
        arrVal[i] = arrVal[i - num_values];
    }

    // overwrite values [index..index+num_values-1]
    for (int64_t i=index; i < index + static_cast<int64_t>(num_values); i++) {
        // Need to make a copy of the value to insert because after the value is assigned,
        // is is moved and can no longer be assigned into anther value.
        JValue copy(insertVals[i - index].GetJValue(), allocator);
        arrVal[i] = copy;
    }

    vec.push_back(arrVal.Size());
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_array_insert(ValkeyModuleCtx *ctx, JDocument *doc, const char *path, int64_t index,
                              const char **jsons, size_t *json_lens, const size_t num_values,
                              jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no array value is selected
        if (!has_array_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }

    // parse json values
    jsn::vector<JParser> insertVals(num_values);
    size_t totalJValueSize = 0;
    for (size_t i=0; i < num_values; i++) {
        if (insertVals[i].Parse(jsons[i], json_lens[i]).HasParseError()) {
            return insertVals[i].GetParseErrorCode();
        }
        CHECK_DOCUMENT_PATH_LIMIT(ctx, selector, insertVals[i])
        totalJValueSize += insertVals[i].GetJValueSize();
    }
    CHECK_DOCUMENT_SIZE_LIMIT(ctx, doc->size, totalJValueSize)

    for (auto &v : selector.getUniqueResultSet()) {
        if (v.first->IsArray()) {
            rc = internal_array_insert(*v.first, insertVals, num_values, index, vec);
            if (rc != JSONUTIL_SUCCESS) return rc;
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-array value
        }
    }
    return JSONUTIL_SUCCESS;
}

STATIC void internal_array_trim(JValue &arrVal, int64_t start, int64_t stop, jsn::vector<size_t> &vec) {
    int64_t size = static_cast<int64_t>(arrVal.Size());
    if (size == 0) {
        vec.push_back(0);
        return;
    }

    // if start < 0, set it to 0.
    if (start < 0) start = 0;

    // if stop >= size, set it to size-1
    if (stop >= size) stop = size - 1;

    if (start >= size || start > stop) {
        // If start >= size or start > stop, empty the array and return *new_len as 0.
        arrVal.Erase(arrVal.Begin(), arrVal.End());
        vec.push_back(0);
        return;
    }

    if (stop < size-1)
        arrVal.Erase(arrVal.Begin() + stop + 1, arrVal.Begin() + size);
    if (start > 0)
        arrVal.Erase(arrVal.Begin(), arrVal.Begin() + start);

    vec.push_back(arrVal.Size());
}

JsonUtilCode dom_array_trim(JDocument *doc, const char *path, int64_t start, int64_t stop,
                            jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) return rc;

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path:
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no array value is selected
        if (!has_array_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }

    for (auto &v : selector.getUniqueResultSet()) {
        if (v.first->IsArray()) {
            internal_array_trim(*v.first, start, stop, vec);
        } else {
            vec.push_back(SIZE_MAX);  // indicates non-array value
        }
    }
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_clear(JDocument *doc, const char *path, size_t &elements_cleared) {
    elements_cleared = 0;
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    if (rc != JSONUTIL_SUCCESS) return rc;

    for (auto &v : selector.getUniqueResultSet()) {
        if (v.first->IsArray()) {
            if (!v.first->Empty()) {
                v.first->Erase(v.first->Begin(), v.first->End());
                elements_cleared++;
            }
        } else if (v.first->IsObject()) {
            if (!v.first->ObjectEmpty()) {
                v.first->RemoveAllMembers();
                elements_cleared++;
            }
        } else if (v.first->IsBool()) {
            if (v.first->IsTrue()) {
                v.first->SetBool(false);
                elements_cleared++;
            }
        } else if (v.first->IsString()) {
            if (v.first->GetStringLength() > 0) {
                v.first->SetString("");
                elements_cleared++;
            }
        } else if (v.first->IsInt()) {
            if (v.first->GetInt() != 0) {
                v.first->SetInt(0);
                elements_cleared++;
            }
        } else if (v.first->IsInt64()) {
            if (v.first->GetInt64() !=0) {
                v.first->SetInt64(0);
                elements_cleared++;
            }
        } else if (v.first->IsUint()) {
            if (v.first->GetUint() != 0) {
                v.first->SetUint(0);
                elements_cleared++;
            }
        } else if (v.first->IsUint64()) {
            if (v.first->GetUint64() != 0) {
                v.first->SetUint64(0);
                elements_cleared++;
            }
        } else if (v.first->IsDouble()) {
            if (v.first->GetDouble() < 0.0 || v.first->GetDouble() > 0.0) {
                v.first->SetDouble("0.0", 3, allocator);
                elements_cleared++;
            }
        }
    }
    return JSONUTIL_SUCCESS;
}

STATIC void internal_array_index_of(const JValue &arrVal, const JValue &inputVal, int64_t start, int64_t stop,
                                    jsn::vector<int64_t> &vec) {
    int64_t size = static_cast<int64_t>(arrVal.Size());
    if (size == 0) {
        vec.push_back(-1);
        return;
    }

    // if stop == 0 or -1, the last element is included.
    if (stop == 0 || stop == -1) stop = size;

    // if stop > size, set it to size.
    if (stop > size) stop = size;

    if (start > stop) {
        vec.push_back(-1);
        return;
    }

    for (int64_t i=start; i < stop; i++) {
        if (arrVal[i] == inputVal) {
            vec.push_back(i);
            return;
        }
    }

    vec.push_back(-1);  // not found
}

JsonUtilCode dom_array_index_of(JDocument *doc, const char *path, const char *scalar_val,
                                const size_t scalar_val_len, int64_t start, int64_t stop,
                                jsn::vector<int64_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    jsn::vector<JValue*> values;
    selector.getSelectedValues(values);

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (values.empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
        // return WRONGTYPE error if no array value is selected
        if (!has_array_value(values)) return JSONUTIL_JSON_ELEMENT_NOT_ARRAY;
    }

    // if start < 0, set it to 0.
    if (start < 0) start = 0;

    // verify the input value is valid JSON
    JParser inputVal;
    if (inputVal.Parse(scalar_val, scalar_val_len).HasParseError()) return inputVal.GetParseErrorCode();

    for (auto &v : values) {
        if (v->IsArray()) {
            internal_array_index_of(*v, inputVal.GetJValue(), start, stop, vec);
        } else {
            vec.push_back(INT64_MAX);  // indicates non-array value
        }
    }
    return JSONUTIL_SUCCESS;
}

JsonUtilCode dom_value_type(JDocument *doc, const char *path, jsn::vector<jsn::string> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (selector.getResultSet().empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
    }

    // JSON type name returned to client for command JSON.TYPE
    static const char *TYPE_NAMES[] = {"null", "boolean", "string", "number", "integer", "object", "array"};

    for (auto &v : selector.getResultSet()) {
        switch (v.first->GetType()) {
            case rapidjson::kNullType:
                vec.push_back(std::move(jsn::string(TYPE_NAMES[0])));
                break;
            case rapidjson::kTrueType:
            case rapidjson::kFalseType:
                vec.push_back(std::move(jsn::string(TYPE_NAMES[1])));
                break;
            case rapidjson::kStringType:
                vec.push_back(std::move(jsn::string(TYPE_NAMES[2])));
                break;
            case rapidjson::kNumberType: {
                if (v.first->IsDouble())
                    vec.push_back(std::move(jsn::string(TYPE_NAMES[3])));
                else
                    vec.push_back(std::move(jsn::string(TYPE_NAMES[4])));
                break;
            }
            case rapidjson::kObjectType:
                vec.push_back(std::move(jsn::string(TYPE_NAMES[5])));
                break;
            case rapidjson::kArrayType:
                vec.push_back(std::move(jsn::string(TYPE_NAMES[6])));
                break;
            default:
                ValkeyModule_Assert(false);
                break;
        }
    }
    return JSONUTIL_SUCCESS;
}

STATIC void dom_reply_with_resp_internal(ValkeyModuleCtx *ctx, const JValue& val) {
    switch (val.GetType()) {
        case rapidjson::kObjectType: {
            ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
            ValkeyModule_ReplyWithSimpleString(ctx, "{");
            long len = 1;
            for (auto &m : val.GetObject()) {
                ValkeyModule_ReplyWithArray(ctx, 2);
                ValkeyModule_ReplyWithStringBuffer(ctx, m.name.GetString(), m.name.GetStringLength());
                dom_reply_with_resp_internal(ctx, m.value);
                len++;
            }
            ValkeyModule_ReplySetArrayLength(ctx, len);
            break;
        }
        case rapidjson::kArrayType: {
            ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
            ValkeyModule_ReplyWithSimpleString(ctx, "[");
            for (auto &m : val.GetArray()) {
                dom_reply_with_resp_internal(ctx, m);
            }
            ValkeyModule_ReplySetArrayLength(ctx, val.Size() + 1);
            break;
        }
        case rapidjson::kNullType:
            ValkeyModule_ReplyWithNull(ctx);
            break;
        case rapidjson::kTrueType:
            ValkeyModule_ReplyWithSimpleString(ctx, "true");
            break;
        case rapidjson::kFalseType:
            ValkeyModule_ReplyWithSimpleString(ctx, "false");
            break;
        case rapidjson::kNumberType: {
            if (val.IsInt()) {
                ValkeyModule_ReplyWithLongLong(ctx, val.GetInt());
            } else if (val.IsInt64()) {
                ValkeyModule_ReplyWithLongLong(ctx, val.GetInt64());
            } else if (val.IsUint()) {
                ValkeyModule_ReplyWithLongLong(ctx, val.GetUint());
            } else if (val.IsUint64()) {
                ValkeyModule_ReplyWithLongLong(ctx, val.GetUint64());
            } else {
                ValkeyModule_Assert(val.IsDouble());
                char str[BUF_SIZE_DOUBLE_RAPID_JSON];
                size_t len = jsonutil_double_to_string_rapidjson(val.GetDouble(), str, sizeof(str));
                ValkeyModule_ReplyWithStringBuffer(ctx, str, len);
            }
            break;
        }
        case rapidjson::kStringType:
            ValkeyModule_ReplyWithStringBuffer(ctx, val.GetString(), val.GetStringLength());
            break;
        default:
            ValkeyModule_Assert(false);
            break;
    }
}

JsonUtilCode dom_reply_with_resp(ValkeyModuleCtx *ctx, JDocument *doc, const char *path) {
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    if (selector.getResultSet().empty()) {
        if (!selector.isV2Path) {
            // Legacy path, return NONEXISTENT
            return JSONUTIL_JSON_PATH_NOT_EXIST;
        } else {
            // JSONPath, return empty array
            ValkeyModule_ReplyWithEmptyArray(ctx);
            return JSONUTIL_SUCCESS;
        }
    }

    if (selector.isV2Path) ValkeyModule_ReplyWithArray(ctx, selector.getResultSet().size());

    for (auto &v : selector.getResultSet()) {
        dom_reply_with_resp_internal(ctx, *v.first);
    }
    return JSONUTIL_SUCCESS;
}

STATIC size_t mem_size_internal(const JValue& v) {
    size_t size = sizeof(v);  // data structure size
    if (v.IsString()) {
        size += v.IsShortString() ? 0 : v.GetStringLength();  // add scalar string value's length
    } else if (v.IsDouble()) {
        size += v.IsShortDouble() ? 0 : v.GetDoubleStringLength();
    } else if (v.IsObject()) {
        for (auto m = v.MemberBegin(); m != v.MemberEnd(); ++m) {
            size += m.NodeSize() - sizeof(m->value);  // Overhead (not including the value, which gets added below)
            size += m->name.GetStringLength();    // add key's length
            size += mem_size_internal(m->value);  // add value's size
        }
    } else if (v.IsArray()) {
        for (auto &m : v.GetArray())
            size += mem_size_internal(m);  // add member's size
    }
    return size;
}

JsonUtilCode dom_mem_size(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path,
                          bool default_path) {
    vec.clear();
    // Optimization:
    // The size of the whole document should be obtained from the meta data attached to document object.
    if (jsonutil_is_root_path(path) && default_path) {
        vec.push_back(dom_get_doc_size(doc));
        is_v2_path = !strcmp(path, "$");
        return JSONUTIL_SUCCESS;
    }

    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (selector.getResultSet().empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
    }

    for (auto &v : selector.getResultSet()) {
        vec.push_back(mem_size_internal(*v.first));
    }
    return JSONUTIL_SUCCESS;
}

STATIC size_t num_fields_internal(JValue& v) {
    size_t num_fields = 1;
    if (v.IsObject()) {
        for (auto &m : v.GetObject())
            num_fields += num_fields_internal(m.value);
    } else if (v.IsArray()) {
        for (auto &m : v.GetArray())
            num_fields += num_fields_internal(m);
    }
    return num_fields;
}

/*
 * If the top-level JSON value is a container (object or array), we want to return number of fields IN the container,
 * not to count the container itself. For a nested container object, we want to count the container itself.
 * e.g., { "address": { "street": "21 2nd Street", "city": "New York", "state": "NY", "zipcode": "10021-3100" } }.
 * If we are counting number of fields in the root doc, the address field is counted. So, there are 5 fields.
 * But If we are counting number of fields for the "address" value, the answer is 4, excluding address field itself.
 */
JsonUtilCode dom_num_fields(JDocument *doc, const char *path, jsn::vector<size_t> &vec, bool &is_v2_path) {
    vec.clear();
    Selector selector;
    JsonUtilCode rc = selector.getValues(doc->GetJValue(), path);
    is_v2_path = selector.isV2Path;
    if (rc != JSONUTIL_SUCCESS) {
        if (selector.isLegacyJsonPathSyntax()) return rc;
        // For v2 path, return error code only if it's a syntax error.
        if (selector.isSyntaxError(rc)) return rc;
    }

    // Legacy path
    if (!is_v2_path) {
        // return NONEXISTENT error if no value is selected
        if (selector.getResultSet().empty()) return JSONUTIL_JSON_PATH_NOT_EXIST;
    }

    for (auto &v : selector.getResultSet()) {
        size_t count = num_fields_internal(*v.first);
        if (v.first->IsObject() || v.first->IsArray())
            count--;  // exclude the container itself
        vec.push_back(count);
    }
    return JSONUTIL_SUCCESS;
}

STATIC void find_path_depth_internal(JValue& v, size_t d, size_t *max_depth) {
    *max_depth = std::max(d, *max_depth);
    if (v.IsObject()) {
        for (auto &m : v.GetObject())
            find_path_depth_internal(m.value, d+1, max_depth);
    } else if (v.IsArray()) {
        for (auto &m : v.GetArray())
            find_path_depth_internal(m, d+1, max_depth);
    }
}

void dom_path_depth(JDocument *doc, size_t *depth) {
    *depth = 0;
    find_path_depth_internal(doc->GetJValue(), 0, depth);
}

/*
 * Make a copy of this document
 */
JDocument *dom_copy(const JDocument *src) {
    int64_t begin_val = jsonstats_begin_track_mem();

    JDocument *dst = create_doc();
    dst->CopyFrom(*src, allocator);

    int64_t delta = jsonstats_end_track_mem(begin_val);
    ValkeyModule_Assert(delta > 0);
    dom_set_doc_size(dst, static_cast<size_t>(delta));

    return dst;
}

/*
 * RDB File Format.
 *
 * Each JValue in RDB file format has a type code followed by type-specific data
 */
enum meta_codes {
    JSON_METACODE_NULL    = 0x01,   // Nothing follows
    JSON_METACODE_STRING  = 0x02,   // Followed by the string
    JSON_METACODE_DOUBLE  = 0x04,   // Followed by the double
    JSON_METACODE_INTEGER = 0x08,   // Coded as a 64-bit Signed Integer
    JSON_METACODE_BOOLEAN = 0x10,   // Coded as the string '1' or '0'
    JSON_METACODE_OBJECT  = 0x20,   // Followed by a member count, and then N "pairs"
    JSON_METACODE_ARRAY   = 0x40,   // Followed by a element count and then n JValue elements
    JSON_METACODE_PAIR    = 0x80    // Codes an object Memory, a string(member name) and a JValue
};

//
// save a JValue, recurse as required for object and array
//
STATIC void store_JValue(ValkeyModuleIO *rdb, const JValue *val) {
    if (val->IsNull()) {
        ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_NULL);
    } else if (val->IsString()) {
        ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_STRING);
        ValkeyModule_SaveStringBuffer(rdb, val->GetString(), val->GetStringLength());
    } else if (val->IsNumber()) {
        if (val->IsDouble()) {
            ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_DOUBLE);
            ValkeyModule_SaveDouble(rdb, val->GetDouble());
        } else {
            ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_INTEGER);
            if (val->IsInt64() || val->IsInt()) {
                ValkeyModule_SaveSigned(rdb, val->GetInt64());
            } else {
                // rdb format doesn't understand unsigned, fail on numbers that aren't handled correctly
                ValkeyModule_Assert(val->GetUint64() < static_cast<uint64_t>(1L << 63));
                ValkeyModule_SaveUnsigned(rdb, val->GetUint64());
            }
        }
    } else if (val->IsFalse()) {
        ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_BOOLEAN);
        ValkeyModule_SaveStringBuffer(rdb, "0", 1);
    } else if (val->IsTrue()) {
        ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_BOOLEAN);
        ValkeyModule_SaveStringBuffer(rdb, "1", 1);
    } else if (val->IsObject()) {
        ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_OBJECT);
        ValkeyModule_SaveUnsigned(rdb, val->MemberCount());
        for (auto m = val->MemberBegin(); m != val->MemberEnd(); ++m) {
            ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_PAIR);
            ValkeyModule_SaveStringBuffer(rdb, m->name.GetString(), m->name.GetStringLength());
            store_JValue(rdb, &m->value);
        }
    } else if (val->IsArray()) {
        ValkeyModule_SaveUnsigned(rdb, JSON_METACODE_ARRAY);
        ValkeyModule_SaveUnsigned(rdb, val->Size());
        for (size_t i = 0; i < val->Size(); ++i) {
            store_JValue(rdb, &(*val)[i]);
        }
    } else {
        ValkeyModule_Assert(false);
    }
}

void dom_save(const JDocument *doc, ValkeyModuleIO *rdb, int encver) {
    switch (encver) {
        case 3: {
            rapidjson::StringBuffer oss;
            serialize_value(*(doc), 0, nullptr, oss);
            ValkeyModule_SaveStringBuffer(rdb, oss.GetString(), oss.GetLength());
            break;
        }
        case 0:
            store_JValue(rdb, doc);
            break;
        default:
            ValkeyModule_Assert(0);
            break;
    }
}

// Helper function, read string into a JValue
STATIC JValue readStringAsJValue(ValkeyModuleIO *rdb) {
    // The modern take is that we have doubles as strings
    size_t str_len;
    char *str = ValkeyModule_LoadStringBuffer(rdb, &str_len);
    if (str) {
        JValue v(str, str_len, allocator);
        ValkeyModule_Free(str);
        return v;
    } else {
        ValkeyModule_LogIOError(rdb, "error", "Unable to read string or double");
        return JValue();
    }
}

// Helper function, read legacy double into a new string double JValue
STATIC JValue readLegacyDoubleAsJValue(ValkeyModuleIO *rdb) {
    double d = ValkeyModule_LoadDouble(rdb);
    char str[BUF_SIZE_DOUBLE_JSON];
    size_t str_len = jsonutil_double_to_string(d, str, sizeof(str));
    if (str) {
        JValue v(str, str_len, allocator, false, /* isdouble */ true);
        return v;
    } else {
        ValkeyModule_LogIOError(rdb, "error", "Unable to read legacy double");
        return JValue();
    }
}

/*
 * One instance of this is passed to all recursive invokations of rdbLoadJValue
 */
typedef struct load_params {
    ValkeyModuleIO *rdb;
    unsigned nestLevel;
    JsonUtilCode status;
} load_params;

JValue rdbLoadJValue(load_params *params) {
    uint64_t code = ValkeyModule_LoadUnsigned(params->rdb);
    switch (code) {
        case JSON_METACODE_NULL:
            return JValue();
        case JSON_METACODE_STRING:
            return readStringAsJValue(params->rdb);
        case JSON_METACODE_DOUBLE:
            return readLegacyDoubleAsJValue(params->rdb);
        case JSON_METACODE_INTEGER:
            return JValue(ValkeyModule_LoadSigned(params->rdb));
        case JSON_METACODE_BOOLEAN: {
            size_t strlen;
            char *s = ValkeyModule_LoadStringBuffer(params->rdb, &strlen);
            char c = (s && strlen == 1) ? *s : 0;
            ValkeyModule_Free(s);
            switch (c) {
                case '1': return JValue(true);
                case '0': return JValue(false);
                default:
                    params->status = JSONUTIL_INVALID_RDB_FORMAT;
                    ValkeyModule_LogIOError(params->rdb, "error", "invalid boolean format");
                    return JValue();
            }
        }
        case JSON_METACODE_OBJECT: {
            uint64_t members = ValkeyModule_LoadUnsigned(params->rdb);
            JValue obj;
            obj.SetObject();
            if (params->nestLevel >= json_get_max_path_limit()) {
                ValkeyModule_LogIOError(params->rdb, "error", "document path limit exceeded");
                params->status = JSONUTIL_DOCUMENT_PATH_LIMIT_EXCEEDED;
                return JValue();
            }
            params->nestLevel++;
            while (members--) {
                uint64_t paircode = ValkeyModule_LoadUnsigned(params->rdb);
                if (paircode != JSON_METACODE_PAIR) {
                    params->status = JSONUTIL_INVALID_RDB_FORMAT;
                    ValkeyModule_LogIOError(params->rdb, "error", "Invalid pair code");
                    params->nestLevel--;
                    return JValue();
                }
                JValue key = readStringAsJValue(params->rdb);
                JValue value = rdbLoadJValue(params);
                obj.AddMember(key, value, allocator);
            }
            params->nestLevel--;
            return obj;
        }
        case JSON_METACODE_ARRAY: {
            uint64_t length = ValkeyModule_LoadUnsigned(params->rdb);
            JValue array;
            array.SetArray();
            array.Reserve(length, allocator);
            if (params->nestLevel >= json_get_max_path_limit()) {
                params->status = JSONUTIL_DOCUMENT_PATH_LIMIT_EXCEEDED;
                ValkeyModule_LogIOError(params->rdb, "error", "document path limit exceeded");
                return JValue();
            }
            params->nestLevel++;
            while (length--) {
                array.PushBack(rdbLoadJValue(params), allocator);
            }
            params->nestLevel--;
            return array;
        }
        default:
            ValkeyModule_LogIOError(params->rdb, "error", "Invalid metadata code %lx", code);
            params->status = JSONUTIL_INVALID_RDB_FORMAT;
            return JValue();
    }
}

JsonUtilCode dom_load(JDocument **doc, ValkeyModuleIO *ctx, int encver) {
    *doc = nullptr;
    ValkeyModule_Log(nullptr, "debug", "Begin dom_load, encver:%d", encver);
    switch (encver) {
        case 3: {
            //
            // New encoding, data is stored as wire-format JSON
            //
            size_t json_len;
            char *json = ValkeyModule_LoadStringBuffer(ctx, &json_len);
            if (!json) return JSONUTIL_INVALID_RDB_FORMAT;
            JsonUtilCode rc = dom_parse(nullptr, json, json_len, doc);
            ValkeyModule_Free(json);
            return rc;
        }
        case 0: {
            //
            // Encoding Version 0, Data is stored JSON node by node.
            //
            load_params params;
            params.rdb = ctx;
            params.nestLevel = 0;
            params.status = JSONUTIL_SUCCESS;
            JValue loadedValue = rdbLoadJValue(&params);
            if (params.status == JSONUTIL_SUCCESS) {
                *doc = create_doc();
                (*doc)->SetJValue(loadedValue);
            }
            return params.status;
        }
        default:
            ValkeyModule_Log(nullptr, "warning", "JSON: Unrecognized rdb encoding level %d", encver);
            return JSONUTIL_INVALID_RDB_FORMAT;
    }
}

//
// Compute Digest
//
STATIC void compute_digest(ValkeyModuleDigest *ctx, const JValue& v) {
    switch (v.GetType()) {
        case rapidjson::Type::kNullType:
            ValkeyModule_DigestAddLongLong(ctx, -1);
            ValkeyModule_DigestEndSequence(ctx);
            break;
        case rapidjson::Type::kFalseType:
            ValkeyModule_DigestAddLongLong(ctx, 0);
            ValkeyModule_DigestEndSequence(ctx);
            break;
        case rapidjson::Type::kTrueType:
            ValkeyModule_DigestAddLongLong(ctx, 1);
            ValkeyModule_DigestEndSequence(ctx);
            break;
        case rapidjson::Type::kArrayType:
            ValkeyModule_DigestAddLongLong(ctx, v.Size());
            ValkeyModule_DigestEndSequence(ctx);
            for (size_t i = 0; i < v.Size(); ++i) {
                compute_digest(ctx, v[i]);
            }
            break;
        case rapidjson::Type::kNumberType:
            if (v.IsDouble()) {
                double d = v.GetDouble();
                int64_t bits;
                memcpy(&bits, &d, sizeof(bits));
                ValkeyModule_DigestAddLongLong(ctx, bits);
            } else if (v.IsUint64()) {
                uint64_t ui = v.GetUint64();
                int64_t bits;
                memcpy(&bits, &ui, sizeof(bits));
                ValkeyModule_DigestAddLongLong(ctx, bits);
            } else {
                ValkeyModule_DigestAddLongLong(ctx, v.GetInt64());
            }
            ValkeyModule_DigestEndSequence(ctx);
            break;
        case rapidjson::Type::kObjectType:
            ValkeyModule_DigestAddLongLong(ctx, v.MemberCount());
            ValkeyModule_DigestEndSequence(ctx);
            for (auto m = v.MemberBegin(); m != v.MemberEnd(); ++m) {
                const char *b = m->name.GetString();
                ValkeyModule_DigestAddStringBuffer(ctx, b, m->name.GetStringLength());
                compute_digest(ctx, m->value);
            }
            break;
        case rapidjson::Type::kStringType:
            ValkeyModule_DigestAddStringBuffer(ctx, v.GetString(), v.GetStringLength());
            ValkeyModule_DigestEndSequence(ctx);
            break;
        default:
            ValkeyModule_Assert(false);
            break;
    }
}

void dom_compute_digest(ValkeyModuleDigest *ctx, const JDocument *doc) {
    compute_digest(ctx, doc->GetJValue());
}

void dom_dump_value(JValue &v) {
    (void)v;
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    v.Accept(writer);
    std::cout << "DEBUG DOM\tvalue: " << sb.GetString() << std::endl;
}

/* ========================= functions consumed by unit tests ======================== */

jsn::string dom_get_string(JDocument *d) {
    return d->GetString();
}

