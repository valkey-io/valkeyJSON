#ifndef _RAPIDJSON_INCLUDES_H
#define _RAPIDJSON_INCLUDES_H

/*
 * This file includes all RapidJSON Files (modified or original). Any RAPIDJSON-global #defines, etc. belong here
 */

#if defined(__amd64__) || defined(__amd64) || defined(__x86_64__) || \
    defined(__x86_64) || defined(_M_X64) || defined(_M_AMD64)
#define RAPIDJSON_SSE42 1
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#define RAPIDJSON_NEON 1
#endif

#define RAPIDJSON_48BITPOINTER_OPTIMIZATION 1

#include "rapidjson/prettywriter.h"
#include "rapidjson/document.h"
#include <rapidjson/encodings.h>

#endif
