#ifndef VALKEYJSONMODULE_JSON_H_
#define VALKEYJSONMODULE_JSON_H_

#include <stddef.h>

size_t json_get_max_document_size();
size_t json_get_defrag_threshold();
size_t json_get_max_path_limit();
size_t json_get_max_parser_recursion_depth();
size_t json_get_max_recursive_descent_tokens();
size_t json_get_max_query_string_size();

bool json_is_instrument_enabled_insert();
bool json_is_instrument_enabled_update();
bool json_is_instrument_enabled_delete();
bool json_is_instrument_enabled_dump_doc_before();
bool json_is_instrument_enabled_dump_doc_after();
bool json_is_instrument_enabled_dump_value_before_delete();

#define DOUBLE_CHARS_CUTOFF 24

#endif  // VALKEYJSONMODULE_JSON_H_
