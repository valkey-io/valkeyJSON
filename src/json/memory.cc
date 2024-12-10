#include <stdlib.h>
#include <string.h>

#include <atomic>
#include <iostream>

#include "json/memory.h"
#include "json/dom.h"

extern "C" {
#define VALKEYMODULE_EXPERIMENTAL_API
#include <./include/valkeymodule.h>
}

#define STATIC /* decorator for static functions, remove so that backtrace symbols include these */

void *(*memory_alloc)(size_t size);
void (*memory_free)(void *ptr);
void *(*memory_realloc)(void *orig_ptr, size_t new_size);
size_t (*memory_allocsize)(void *ptr);

bool memoryTrapsEnabled = false;

static std::atomic<size_t> totalMemoryUsage;

size_t memory_usage() {
    return totalMemoryUsage;
}

/*
 * When Traps are disabled, The following code is used
 */

STATIC void *memory_alloc_without_traps(size_t size) {
    void *ptr = ValkeyModule_Alloc(size);
    totalMemoryUsage += ValkeyModule_MallocSize(ptr);
    return ptr;
}

STATIC void memory_free_without_traps(void *ptr) {
    if (!ptr) return;
    size_t sz = ValkeyModule_MallocSize(ptr);
    ValkeyModule_Assert(sz <= totalMemoryUsage);
    totalMemoryUsage -= sz;
    ValkeyModule_Free(ptr);
}

STATIC void *memory_realloc_without_traps(void *ptr, size_t new_size) {
    if (ptr) {
        size_t old_size = ValkeyModule_MallocSize(ptr);
        ValkeyModule_Assert(old_size <= totalMemoryUsage);
        totalMemoryUsage -= old_size;
    }
    ptr = ValkeyModule_Realloc(ptr, new_size);
    totalMemoryUsage += ValkeyModule_MallocSize(ptr);
    return ptr;
}

#define memory_allocsize_without_traps ValkeyModule_MallocSize

//
// Implementation of traps
//

//
// This word of data preceeds the memory allocation as seen by the client.
// The presence of the length is redundant with calling the low-level allocators memory-size function,
// but that function can be fairly expensive, so by duplicating here we optimize the run-time cost.
//
struct trap_prefix {
    mutable uint64_t length:40;
    mutable uint64_t valid_prefix:24;
    enum { VALID = 0xdeadbe, INVALID = 0xf00dad};
    static       trap_prefix *from_ptr(      void *p) { return reinterpret_cast<      trap_prefix *>(p) - 1; }
    static const trap_prefix *from_ptr(const void *p) { return reinterpret_cast<const trap_prefix *>(p) - 1; }
};

//
// Another word of data is added to end of each allocation. It's set to a known data pattern.
//
struct trap_suffix {
    mutable uint64_t valid_suffix;
    enum { VALID = 0xdeadfeedbeeff00dull, INVALID = ~VALID };
    static trap_suffix *from_prefix(trap_prefix *p) {
        return reinterpret_cast<trap_suffix *>(p + 1 + (p->length >> 3));
    }
    static const trap_suffix *from_prefix(const trap_prefix *p) {
        return reinterpret_cast<const trap_suffix *>(p + 1 + (p->length >> 3));
    }
};

bool memory_validate_ptr(const void *ptr, bool crashOnError) {
    if (!ptr) return true;   // Null pointers are valid.
    auto prefix = trap_prefix::from_ptr(ptr);
    if (prefix->valid_prefix != trap_prefix::VALID) {
        if (crashOnError) {
            ValkeyModule_Log(nullptr, "error", "Validation Failure memory Corrupted at:%p", ptr);
            ValkeyModule_Assert(nullptr == "Validate Prefix Corrupted");
        } else {
            return false;
        }
    }
    auto suffix = trap_suffix::from_prefix(prefix);
    if (suffix->valid_suffix != trap_suffix::VALID) {
        if (!crashOnError) return false;
        // Dump the first N bytes. Hopefully this might give us a clue what's going wrong....
        size_t malloc_size = ValkeyModule_MallocSize(const_cast<void *>(reinterpret_cast<const void *>(prefix)));
        ValkeyModule_Assert(malloc_size >= (sizeof(trap_prefix) + sizeof(trap_suffix)));
        size_t available_size = malloc_size - (sizeof(trap_prefix) + sizeof(trap_suffix));
        size_t dump_size = available_size > 256 ? 256 : available_size;
        ValkeyModule_Log(nullptr, "error", "Validation Failure memory overrun @%p size:%zu", ptr, available_size);
        auto data = static_cast<const void * const*>(ptr);
        while (dump_size > (4 * sizeof(void *))) {
            ValkeyModule_Log(nullptr, "error", "Memory[%p]: %p %p %p %p",
                            static_cast<const void *>(data), data[0], data[1], data[2], data[3]);
            data += 4;
            dump_size -= 4 * sizeof(void *);
        }
        while (dump_size) {
            ValkeyModule_Log(nullptr, "error", "Memory[%p]: %p",
                            static_cast<const void *>(data), data[0]);
            data++;
            dump_size -= sizeof(void *);
        }
        ValkeyModule_Assert(nullptr == "Validate Suffix Corrupted");
    }
    return true;
}

STATIC void *memory_alloc_with_traps(size_t size) {
    size_t requested_bytes = ~7 & (size + 7);  // Round up
    size_t alloc_bytes = requested_bytes + sizeof(trap_prefix) + sizeof(trap_suffix);
    auto prefix = reinterpret_cast<trap_prefix *>(ValkeyModule_Alloc(alloc_bytes));
    totalMemoryUsage += ValkeyModule_MallocSize(prefix);
    prefix->valid_prefix = trap_prefix::VALID;
    prefix->length = requested_bytes;
    auto suffix = trap_suffix::from_prefix(prefix);
    suffix->valid_suffix = trap_suffix::VALID;
    return reinterpret_cast<void *>(prefix + 1);
}

STATIC void memory_free_with_traps(void *ptr) {
    if (!ptr) return;
    memory_validate_ptr(ptr);
    auto prefix = trap_prefix::from_ptr(ptr);
    prefix->valid_prefix = 0;
    size_t sz = ValkeyModule_MallocSize(prefix);
    ValkeyModule_Assert(sz <= totalMemoryUsage);
    totalMemoryUsage -= sz;
    ValkeyModule_Free(prefix);
}

STATIC size_t memory_allocsize_with_traps(void *ptr) {
    if (!ptr) return 0;
    memory_validate_ptr(ptr);
    auto prefix = trap_prefix::from_ptr(ptr);
    return prefix->length;
}

//
// Do a realloc, but this is rare, so we do it suboptimally, i.e., with a copy
//
STATIC void *memory_realloc_with_traps(void *orig_ptr, size_t new_size) {
    if (!orig_ptr) return memory_alloc_with_traps(new_size);
    memory_validate_ptr(orig_ptr);
    auto new_ptr = memory_alloc_with_traps(new_size);
    memcpy(new_ptr, orig_ptr, memory_allocsize_with_traps(orig_ptr));
    memory_free_with_traps(orig_ptr);
    return new_ptr;
}

//
// Enable/Disable traps
//
bool memory_traps_control(bool enable) {
    if (totalMemoryUsage != 0) {
        ValkeyModule_Log(nullptr, "warning",
           "Attempt to enable/disable memory traps ignored, %zu outstanding memory.", totalMemoryUsage.load());
        return false;
    }
    if (enable) {
        memory_alloc = memory_alloc_with_traps;
        memory_free = memory_free_with_traps;
        memory_realloc = memory_realloc_with_traps;
        memory_allocsize = memory_allocsize_with_traps;
    } else {
        memory_alloc = memory_alloc_without_traps;
        memory_free = memory_free_without_traps;
        memory_realloc = memory_realloc_without_traps;
        memory_allocsize = memory_allocsize_without_traps;
    }
    memoryTrapsEnabled = enable;
    return true;
}

void memory_corrupt_memory(const void *ptr, memTrapsCorruption_t corruption) {
    memory_validate_ptr(ptr);
    auto prefix = trap_prefix::from_ptr(ptr);
    auto suffix = trap_suffix::from_prefix(prefix);
    switch (corruption) {
        case CORRUPT_PREFIX:
            prefix->valid_prefix = trap_prefix::INVALID;
            break;
        case CORRUPT_LENGTH:
            prefix->length--;
            break;
        case CORRUPT_SUFFIX:
            suffix->valid_suffix = trap_suffix::INVALID;
            break;
        default:
            ValkeyModule_Assert(0);
            break;
    }
}

void memory_uncorrupt_memory(const void *ptr, memTrapsCorruption_t corruption) {
    auto prefix = trap_prefix::from_ptr(ptr);
    auto suffix = trap_suffix::from_prefix(prefix);
    switch (corruption) {
        case CORRUPT_PREFIX:
            ValkeyModule_Assert(prefix->valid_prefix == trap_prefix::INVALID);
            prefix->valid_prefix = trap_prefix::VALID;
            break;
        case CORRUPT_LENGTH:
            prefix->length++;
            break;
        case CORRUPT_SUFFIX:
            ValkeyModule_Assert(suffix->valid_suffix == trap_suffix::INVALID);
            suffix->valid_suffix = trap_suffix::VALID;
            break;
        default:
            ValkeyModule_Assert(0);
            break;
    }
    memory_validate_ptr(ptr);
}

//
// Helper functions for JSON validation
//
// true => Valid.
// false => NOT VALID
//
bool ValidateJValue(JValue &v) {
    auto p = v.trap_GetMallocPointer(false);
    if (p && !memory_validate_ptr(p, false)) return false;
    if (v.IsObject()) {
        for (auto m = v.MemberBegin(); m != v.MemberEnd(); ++m) {
            if (!ValidateJValue(m->value)) return false;
        }
    } else if (v.IsArray()) {
        for (size_t i = 0; i < v.Size(); ++i) {
            if (!ValidateJValue(v[i])) return false;
        }
    }
    return true;
}

//
// Dump a JValue with Redaction and memory Validation.
//
// Typical use case:
//
//  std::ostringstream os;
//  DumpRedactedJValue(os, <jvalue>);
//
void DumpRedactedJValue(std::ostream& os, const JValue &v, size_t level, int index) {
    for (size_t i = 0; i < (3 * level); ++i) os << ' ';  // Indent
    os << "@" << reinterpret_cast<const void *>(&v) << " ";
    if (index != -1) os << '[' << index << ']' << ' ';
    if (v.IsDouble()) {
        os << "double string of length " << v.GetDoubleStringLength();
        if (!IS_VALID_MEMORY(v.trap_GetMallocPointer(false))) {
            os << " <*INVALID*>\n";
        } else if (v.trap_GetMallocPointer(false)) {
            os << " @" << v.trap_GetMallocPointer(false) << "\n";
        } else {
            os << "\n";
        }
    } else if (v.IsString()) {
        os << "String of length " << v.GetStringLength();
        if (!IS_VALID_MEMORY(v.trap_GetMallocPointer(false))) {
            os << " <*INVALID*>\n";
        } else if (v.trap_GetMallocPointer(false)) {
            os << " @" << v.trap_GetMallocPointer(false) << "\n";
        } else {
            os << "\n";
        }
    } else if (v.IsObject()) {
        os << " Object with " << v.MemberCount() << " Members";
        if (!IS_VALID_MEMORY(v.trap_GetMallocPointer(false))) {
            os << " *INVALID*\n";
        } else {
            os << " @" << v.trap_GetMallocPointer(false) << '\n';
            index = 0;
            for (auto m = v.MemberBegin(); m != v.MemberEnd(); ++m) {
                DumpRedactedJValue(os, m->value, level+1, index);
                index++;
            }
        }
    } else if (v.IsArray()) {
        os << "Array with " << v.Size() << " Members";
        if (!IS_VALID_MEMORY(v.trap_GetMallocPointer(false))) {
            os << " *INVALID*\n";
        } else {
            os << " @" << v.trap_GetMallocPointer(false) << "\n";
            for (size_t index = 0; index < v.Size(); ++index) {
                DumpRedactedJValue(os, v[index], level+1, int(index));
            }
        }
    } else {
        os << "<scalar>\n";
    }
}

//
// This class creates an ostream to the Valkey Log. Each line of output is a single call to the ValkeyLog function
//
class ValkeyLogStreamBuf : public std::streambuf {
    std::string line;
    ValkeyModuleCtx *ctx;
    const char *level;

 public:
    ValkeyLogStreamBuf(ValkeyModuleCtx *_ctx, const char *_level) : ctx(_ctx), level(_level) {}
    ~ValkeyLogStreamBuf() {
        if (!line.empty()) {
            ValkeyModule_Log(ctx, level, "%s", line.c_str());
        }
    }
    std::streamsize xsputn(const char *p, std::streamsize n) {
        for (std::streamsize i = 0; i < n; ++i) {
            overflow(p[i]);
        }
        return n;
    }
    int overflow(int c) {
        if (c == '\n' || c == EOF) {
            ValkeyModule_Log(ctx, level, "%s", line.c_str());
            line.resize(0);
        } else {
            line += c;
        }
        return c;
    }
};

void DumpRedactedJValue(const JValue &v, ValkeyModuleCtx *ctx, const char *level) {
    ValkeyLogStreamBuf b(ctx, level);
    std::ostream buf(&b);
    DumpRedactedJValue(buf, v);
}
