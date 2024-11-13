#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>
#include "json/json_api.h"
#include "json/dom.h"
#include "json/memory.h"

extern ValkeyModuleType* DocumentType;

int is_json_key(ValkeyModuleCtx *ctx, ValkeyModuleKey *key) {
    VALKEYMODULE_NOT_USED(ctx);
    if (key == nullptr || ValkeyModule_KeyType(key) == VALKEYMODULE_KEYTYPE_EMPTY) return 0;
    return (ValkeyModule_ModuleTypeGetType(key) == DocumentType? 1: 0);
}

int is_json_key2(ValkeyModuleCtx *ctx, ValkeyModuleString *keystr) {
    ValkeyModuleKey *key = static_cast<ValkeyModuleKey*>(ValkeyModule_OpenKey(ctx, keystr, VALKEYMODULE_READ));
    int is_json = is_json_key(ctx, key);
    ValkeyModule_CloseKey(key);
    return is_json;
}

static JDocument* get_json_document(ValkeyModuleCtx *ctx, const char *keyname, const size_t key_len) {
    ValkeyModuleString *keystr = ValkeyModule_CreateString(ctx, keyname, key_len);
    ValkeyModuleKey *key = static_cast<ValkeyModuleKey*>(ValkeyModule_OpenKey(ctx, keystr, VALKEYMODULE_READ));
    if (!is_json_key(ctx, key)) {
        ValkeyModule_CloseKey(key);
        ValkeyModule_FreeString(ctx, keystr);
        return nullptr;
    }
    JDocument *doc = static_cast<JDocument*>(ValkeyModule_ModuleTypeGetValue(key));
    ValkeyModule_CloseKey(key);
    ValkeyModule_FreeString(ctx, keystr);
    return doc;
}

int get_json_value_type(ValkeyModuleCtx *ctx, const char *keyname, const size_t key_len, const char *path,
                        char **type, size_t *len) {
    *type = nullptr;
    *len = 0;
    JDocument *doc = get_json_document(ctx, keyname, key_len);
    if (doc == nullptr) return -1;

    jsn::vector<jsn::string> vec;
    bool is_v2_path;
    JsonUtilCode rc = dom_value_type(doc, path, vec, is_v2_path);
    if (rc != JSONUTIL_SUCCESS || vec.empty()) return -1;
    *type = static_cast<char*>(ValkeyModule_Alloc(vec[0].length() + 1));
    *len = vec[0].length();
    snprintf(*type, *len + 1, "%s", vec[0].c_str());
    return 0;
}

int get_json_value(ValkeyModuleCtx *ctx, const char *keyname, const size_t key_len, const char *path,
                   char **value, size_t *len) {
    *value = nullptr;
    *len = 0;
    JDocument *doc = get_json_document(ctx, keyname, key_len);
    if (doc == nullptr) return -1;

    rapidjson::StringBuffer buf;
    JsonUtilCode rc = dom_get_value_as_str(doc, path, nullptr, buf, false);
    if (rc != JSONUTIL_SUCCESS) return -1;
    *len = buf.GetLength();
    *value = static_cast<char*>(ValkeyModule_Alloc(*len + 1));
    snprintf(*value, *len + 1, "%s", buf.GetString());
    return 0;
}

int get_json_values_and_types(ValkeyModuleCtx *ctx, const char *keyname, const size_t key_len, const char **paths,
            const int num_paths, char ***values, size_t **lengths, char ***types, size_t **type_lengths) {
    ValkeyModule_Assert(values != nullptr);
    ValkeyModule_Assert(lengths != nullptr);
    *values = nullptr;
    *lengths = nullptr;
    if (types != nullptr) *types = nullptr;
    if (type_lengths != nullptr) *type_lengths = nullptr;
    JDocument *doc = get_json_document(ctx, keyname, key_len);
    if (doc == nullptr) return -1;

    *values = static_cast<char **>(ValkeyModule_Alloc(num_paths * sizeof(char *)));
    *lengths = static_cast<size_t *>(ValkeyModule_Alloc(num_paths * sizeof(size_t)));
    memset(*values, 0, num_paths * sizeof(char *));
    memset(*lengths, 0, num_paths * sizeof(size_t));
    for (int i = 0; i < num_paths; i++) {
        rapidjson::StringBuffer buf;
        JsonUtilCode rc = dom_get_value_as_str(doc, paths[i], nullptr, buf, false);
        if (rc == JSONUTIL_SUCCESS) {
            (*lengths)[i] = buf.GetLength();
            (*values)[i] = static_cast<char*>(ValkeyModule_Alloc((*lengths)[i] + 1));
            snprintf((*values)[i], (*lengths)[i] + 1, "%s", buf.GetString());
        } else {
            (*values)[i] = nullptr;
        }
    }

    if (types != nullptr) {
        ValkeyModule_Assert(type_lengths != nullptr);

        *types = static_cast<char **>(ValkeyModule_Alloc(num_paths * sizeof(char *)));
        *type_lengths = static_cast<size_t *>(ValkeyModule_Alloc(num_paths * sizeof(size_t)));
        memset(*types, 0, num_paths * sizeof(char *));
        memset(*type_lengths, 0, num_paths * sizeof(size_t));
        for (int i = 0; i< num_paths; i++) {
            jsn::vector<jsn::string> vec;
            bool is_v2_path;
            JsonUtilCode rc = dom_value_type(doc, paths[i], vec, is_v2_path);
            if (rc == JSONUTIL_SUCCESS && !vec.empty()) {
                (*type_lengths)[i] = vec[0].length();
                (*types)[i] = static_cast<char*>(ValkeyModule_Alloc((*type_lengths)[i] + 1));
                snprintf((*types)[i], (*type_lengths)[i] + 1, "%s", vec[0].c_str());
            } else {
                (*types)[i] = nullptr;
            }
        }
    }
    return 0;
}
