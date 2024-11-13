cmake_minimum_required(VERSION 3.17)

include(FetchContent)
include(ExternalProject)

# Detect the system architecture
EXECUTE_PROCESS(
    COMMAND uname -m
    COMMAND tr -d '\n'
    OUTPUT_VARIABLE ARCHITECTURE
)

if("${ARCHITECTURE}" STREQUAL "x86_64")
    message("Building JSON for x86_64")
elseif("${ARCHITECTURE}" STREQUAL "aarch64")
    message("Building JSON for aarch64")
else()
    message(FATAL_ERROR "Unsupported architecture. JSON is only supported on x86_64 and aarch64.")
endif()

# Project definition
project(ValkeyJSONModule VERSION 1.0 LANGUAGES C CXX)

# Set the name of the JSON shared library
set(JSON_MODULE_LIB json)

# Define the Valkey directories
set(VALKEY_DOWNLOAD_DIR "${CMAKE_BINARY_DIR}/_deps/valkey-src")
set(VALKEY_BIN_DIR "${CMAKE_BINARY_DIR}/_deps/valkey-src/src/valkey/bin")

# Download and build Valkey
ExternalProject_Add(
    valkey
    GIT_REPOSITORY https://github.com/valkey-io/valkey.git  # Replace with actual URL
    GIT_TAG ${VALKEY_VERSION}
    PREFIX ${VALKEY_DOWNLOAD_DIR}
    BUILD_COMMAND make -j
    INSTALL_COMMAND ""
    BUILD_IN_SOURCE 1
)

# Define the paths for the copied files
set(VALKEY_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/include")
set(VALKEY_BINARY_DEST "${CMAKE_CURRENT_SOURCE_DIR}/tst/integration/.build/binaries/${VALKEY_VERSION}")

ExternalProject_Add_Step(
    valkey
    copy_header_files
    COMMENT "Copying header files to include/ directory"
    DEPENDEES download
    DEPENDERS configure
    COMMAND ${CMAKE_COMMAND} -E make_directory ${VALKEY_INCLUDE_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy ${VALKEY_DOWNLOAD_DIR}/src/valkey/src/valkeymodule.h ${VALKEY_INCLUDE_DIR}/valkeymodule.h
    ALWAYS 1
)

# Copy header and binary after Valkey make
add_custom_command(TARGET valkey
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${VALKEY_BINARY_DEST}
    COMMAND ${CMAKE_COMMAND} -E copy ${VALKEY_BIN_DIR}/valkey-server ${VALKEY_BINARY_DEST}/valkey-server
    COMMENT "Copied valkeymodule.h and valkey-server to destination directories"
)

# Define valkey-bloom branch
set(VALKEY_BLOOM_BRANCH "unstable" CACHE STRING "Valkey-bloom branch to use")

# Set the download directory for Valkey-bloom
set(VALKEY_BLOOM_DOWNLOAD_DIR "${CMAKE_CURRENT_BINARY_DIR}/_deps/valkey-bloom-src")

# Download valkey-bloom
ExternalProject_Add(
    valkey-bloom
    GIT_REPOSITORY https://github.com/valkey-io/valkey-bloom.git
    GIT_TAG ${VALKEY_BLOOM_BRANCH}
    GIT_SHALLOW TRUE
    PREFIX "${VALKEY_BLOOM_DOWNLOAD_DIR}"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

# Step to copy pytest files
ExternalProject_Add_Step(
    valkey-bloom
    copy_pytest_files
    COMMENT "Copying pytest files to tst/integration directory"
    DEPENDEES build
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/tst/integration
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${VALKEY_BLOOM_DOWNLOAD_DIR}/src/valkey-bloom/tests/valkeytests ${CMAKE_CURRENT_SOURCE_DIR}/tst/integration/valkeytests
)

# Enable instrumentation options if requested
if("$ENV{INSTRUMENT_V2PATH}" STREQUAL "yes")
    add_compile_definitions(INSTRUMENT_V2PATH)
    message("Enabled INSTRUMENT_V2PATH")
endif()

# Disable Doxygen documentation generation
set(BUILD_DOCUMENTATION OFF)
# When CODE_COVERAGE is ON, the package is built twice, once for debug and once for release.
# To fix the problem, disable the code coverage.
set(CODE_COVERAGE OFF)

# Fix for linking error when code coverage is enabled on ARM
if(CODE_COVERAGE AND CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_link_options("--coverage")
endif()

# Set C & C++ standard versions
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Always include debug symbols and optimize the code
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")

# RapidJSON SIMD optimization
if("${ARCHITECTURE}" STREQUAL "x86_64")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=nehalem")
elseif("${ARCHITECTURE}" STREQUAL "aarch64")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a")
else()
    message(FATAL_ERROR "Unsupported architecture. JSON is only supported on x86_64 and aarch64.")
endif()

# Additional flags for all architectures
set(ADDITIONAL_FLAGS "-fPIC")

# Compiler warning flags
set(C_WARNING "-Wall -Werror -Wextra")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${ADDITIONAL_FLAGS} ${C_WARNING}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_FLAGS} ${C_WARNING}")

# Fetch RapidJSON
FetchContent_Declare(
    rapidjson
    GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
    GIT_TAG 0d4517f15a8d7167ba9ae67f3f22a559ca841e3b
)

# Disable RapidJSON tests and examples
set(RAPIDJSON_BUILD_TESTS OFF CACHE BOOL "Build rapidjson tests" FORCE)
set(RAPIDJSON_BUILD_EXAMPLES OFF CACHE BOOL "Build rapidjson examples" FORCE)
set(RAPIDJSON_BUILD_DOC OFF CACHE BOOL "Build rapidjson documentation" FORCE)

# Make Rapidjson available
FetchContent_MakeAvailable(rapidjson)

# Add the src subdirectory for building
add_subdirectory(src)

# Add the src subdirectory for building
add_subdirectory(tst)

add_custom_target(test
    COMMENT "Run JSON integration tests."
    USES_TERMINAL
    COMMAND rm -rf ${CMAKE_BINARY_DIR}/tst/integration
    COMMAND mkdir -p ${CMAKE_BINARY_DIR}/tst/integration
    COMMAND cp -rp ${CMAKE_SOURCE_DIR}/tst/integration/. ${CMAKE_BINARY_DIR}/tst/integration/
    COMMAND echo "[TARGET] begin integration tests"
    COMMAND ${CMAKE_SOURCE_DIR}/tst/integration/run.sh "test" ${CMAKE_SOURCE_DIR} 
    COMMAND echo "[TARGET] end integration tests")