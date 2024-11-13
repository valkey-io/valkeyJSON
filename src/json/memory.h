/**
 */
#ifndef VALKEYJSONMODULE_MEMORY_H_
#define VALKEYJSONMODULE_MEMORY_H_

#include <stddef.h>

#include <vector>
#include <set>
#include <unordered_set>
#include <iostream>
#include <string>
#include <sstream>

//
// Trap implementation
//
// Memory traps are a diagnostic tool intended to catch some categories of memory usage errors.
//
// The Trap system is conceptually a shim layer between the client application and the lower level memory allocator.
// Traps operate by adding to each memory allocation a prefix and a suffix. The prefix and suffix contain known
// data patterns and some internal trap metadata. Subsequent memory operations validate the correctness of the
// prefix and suffix. A special interface is provided to allow any client application to voluntarily request
// memory validation -- presumably before utilizing the underlying memory.
//
// This strategy should catch at least three classes of memory corruption:
//
//  (1) double free of memory.
//  (2) writes off the end of memory (just the prev/next word, not WAAAAY off the end of memory)
//  (3) dangling pointer to previously freed memory (this relies on voluntary memory validation)
//
// Traps can be dynamically enabled/disabled, provided that there is no outstanding memory allocation.
//

//
// All functions in the module (outsize of memory.cc) should use these to allocate memory
// instead of the ValkeyModule_xxxx functions.
//
extern void *(*memory_alloc)(size_t size);
extern void (*memory_free)(void *ptr);
extern void *(*memory_realloc)(void *orig_ptr, size_t new_size);
extern size_t (*memory_allocsize)(void *ptr);

//
// Total memory usage.
//
//  (1) Includes dom_alloc memory usage. dom_alloc tracks JSON data that's associated with a document
//  (2) Includes KeyTable usage, i.e., JSON data that's shared across documents
//  (3) Includes STL library allocations
//
extern size_t memory_usage();

//
// Are traps enabled?
//
inline bool memory_traps_enabled() {
    extern bool memoryTrapsEnabled;
    return memoryTrapsEnabled;
}

//
// External Interface to traps logic
//
// Enables/Disable traps. This can fail if there's outstanding allocated memory.
//
// return true => operation was successful.
// return false => operation failed (there's outstanding memory)
//
bool memory_traps_control(bool enable);

bool memory_validate_ptr(const void *ptr, bool crashOnError = true);
//
// This version validates memory, but crashes on an invalid pointer
//
template<typename t>
static inline t *MEMORY_VALIDATE(t *ptr, bool validate = true) {
    extern bool memoryTrapsEnabled;
    if (memoryTrapsEnabled && validate) memory_validate_ptr(ptr, true);
    return ptr;
}

//
// This version validates memory, but doesn't crash
//
template<typename t>
static inline bool IS_VALID_MEMORY(t *ptr) {
    return memory_validate_ptr(ptr, false);
}

//
// Classes for STL Containers that utilize memory usage and trap logic.
//
namespace jsn
{
//
// Our custom allocator
//
template <typename T> class stl_allocator : public std::allocator<T> {
 public:
    typedef T value_type;
    stl_allocator() = default;
    stl_allocator(std::allocator<T>&) {}
    stl_allocator(std::allocator<T>&&) {}
    template <class U> constexpr stl_allocator(const stl_allocator<U>&) noexcept {}

    T *allocate(std::size_t n) { return static_cast<T *>(memory_alloc(n*sizeof(T))); }
    void deallocate(T *p, std::size_t n) { (void)n; memory_free(p); }
};

template<class Elm> using vector = std::vector<Elm, stl_allocator<Elm>>;

template<class Key, class Compare = std::less<Key>> using set = std::set<Key, Compare, stl_allocator<Key>>;

template<class Key, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
        using unordered_set = std::unordered_set<Key, Hash, KeyEqual, stl_allocator<Key>>;

typedef std::basic_string<char, std::char_traits<char>, stl_allocator<char>> string;
typedef std::basic_stringstream<char, std::char_traits<char>, stl_allocator<char>> stringstream;

}  // namespace jsn

// custom specialization of std::hash can be injected in namespace std
template<>
struct std::hash<jsn::string>
{
    std::size_t operator()(const jsn::string& s) const noexcept {
        return std::hash<std::string_view>{}(std::string_view(s.c_str(), s.length()));
    }
};

//
// Everything below this line is private to this module, it's here for usage by unit tests
//

typedef enum MEMORY_TRAPS_CORRUPTION {
    CORRUPT_PREFIX,
    CORRUPT_LENGTH,
    CORRUPT_SUFFIX
} memTrapsCorruption_t;

void memory_corrupt_memory(const void *ptr, memTrapsCorruption_t corrupt);
void memory_uncorrupt_memory(const void *ptr, memTrapsCorruption_t corrupt);

#endif  // VALKEYJSONMODULE_MEMORY_H_
