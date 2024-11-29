message("src/CMakeLists.txt: Build JSON")

set(OBJECT_TARGET json-objects CACHE INTERNAL "Object target for json module")
add_library(${OBJECT_TARGET} OBJECT "")

# Build with C11 & C++17
set_target_properties(
	${OBJECT_TARGET}
	PROPERTIES
	C_STANDARD 11
	C_STANDARD_REQUIRED ON
	CXX_STANDARD 17
	CXX_STANDARD_REQUIRED ON
	POSITION_INDEPENDENT_CODE ON
)

target_include_directories(${OBJECT_TARGET}

	# Need to make the source files public within CMake
	# so that they are used when building the tests.
	PUBLIC
	${CMAKE_CURRENT_SOURCE_DIR}
	${rapidjson_SOURCE_DIR}/include
)

# Add source files for the JSON module
target_sources(${OBJECT_TARGET}
	PRIVATE
	json/json.cc
	json/dom.cc
	json/alloc.cc
	json/util.cc
	json/stats.cc
	json/selector.cc
	json/keytable.cc
	json/memory.cc
	json/json_api.cc
)

add_library(${JSON_MODULE_LIB} SHARED $<TARGET_OBJECTS:${OBJECT_TARGET}>)
