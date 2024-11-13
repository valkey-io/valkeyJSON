import pytest
import os
import random
import string
from valkey.exceptions import ResponseError

JSON_MODULE_NAME = 'json'
JSON_INFO_NAMES = {
    'num_documents':        JSON_MODULE_NAME + '_num_documents',
    'total_memory_bytes':   JSON_MODULE_NAME + '_total_memory_bytes',
    'doc_histogram':        JSON_MODULE_NAME + '_doc_histogram',
    'read_histogram':       JSON_MODULE_NAME + '_read_histogram',
    'insert_histogram':     JSON_MODULE_NAME + '_insert_histogram',
    'update_histogram':     JSON_MODULE_NAME + '_update_histogram',
    'delete_histogram':     JSON_MODULE_NAME + '_delete_histogram',
    'max_path_depth_ever_seen':     JSON_MODULE_NAME + '_max_path_depth_ever_seen',
    'max_document_size_ever_seen':  JSON_MODULE_NAME + '_max_document_size_ever_seen',
    'total_malloc_bytes_used':      JSON_MODULE_NAME + "_total_malloc_bytes_used",
    'memory_traps_enabled':         JSON_MODULE_NAME + "_memory_traps_enabled",
}
DEFAULT_MAX_DOCUMENT_SIZE = 64*1024*1024
DEFAULT_MAX_PATH_LIMIT = 128
DEFAULT_WIKIPEDIA_PATH = 'data/wikipedia.json'
DEFAULT_WIKIPEDIA_COMPACT_PATH = 'data/wikipedia_compact.json'
DEFAULT_STORE_PATH = 'data/store.json'
JSON_INFO_METRICS_SECTION = JSON_MODULE_NAME + '_core_metrics'

JSON_MODULE_NAME = 'json'
