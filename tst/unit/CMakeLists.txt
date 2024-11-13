#########################################
# Define unit tests
#########################################
message("tst/unit/CMakeLists.txt: Define unit tests")

# This is the set of sources for the basic test
file(GLOB_RECURSE UNIT_TEST_SRC "*.cc")

#########################################
# Tell Cmake how to run the unit tests
#########################################

# A brief philosophical thought, about unit tests: if possible, it's preferable to have all unit
# tests in a single (or a low number of) binary executable. This is disk-space efficient for the
# test suite, avoids unnecessary linking steps, and provides a nice, simple way to interface with
# the test suite (should you need to do so manually, or, for instance, with a debugger). Large
# numbers of test binaries are certainly possible, and in some rare cases are even necessary, but
# don't provide many advantages over a single binary in the average case.
# This file defines a single test executable, "unitTests", which uses the Googletest framework.

# This defines each unit test and associates it with its sources
add_executable(unitTests ${UNIT_TEST_SRC})

# Build with C11 & C++17
set_target_properties(
        unitTests
        PROPERTIES
        C_STANDARD 11
        C_STANDARD_REQUIRED ON
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
        POSITION_INDEPENDENT_CODE ON
)

target_include_directories(unitTests
        PRIVATE
        ${PROJECT_SOURCE_DIR}/src
        ${rapidjson_SOURCE_DIR}/include
        )

# Add dependency to the code under test.
target_link_libraries(unitTests ${JSON_MODULE_LIB})

# Link GoogleTest libraries after fetch
target_link_libraries(unitTests 
    GTest::gtest_main        # Link the main GoogleTest library
    GTest::gmock_main        # Link GoogleMock
)

# This tells CTest about this unit test executable
# The TEST_PREFIX prepends "unit_" to the name of these tests in the output,
# which makes them easier to identify at a glance
# The TEST_LIST settings creates a CMake list of all of the tests in the
# binary. This is useful for, for instance, the set_tests_properties statement
# below
# For more information, see: https://cmake.org/cmake/help/v3.12/module/GoogleTest.html
# To get this to work properly in a cross-compile environment, you need to set up
# CROSSCOMPILING_EMULATOR (see https://cmake.org/cmake/help/v3.12/prop_tgt/CROSSCOMPILING_EMULATOR.html)
# DISCOVERY_TIMEOUT - number of seconds given for Gtest to discover the tests to run, it should
# be big enough so the tests can start on MacOS and can be any number, 59 is just prime number
# close to 1 minute ;)
gtest_discover_tests(unitTests
        TEST_PREFIX unit_
        TEST_LIST unit_gtests
        DISCOVERY_TIMEOUT 59
        )

# This tells the CTest harness about how it should treat these tests. For
# instance, you can uncomment the RUN_SERIAL line to force the tests to run
# sequentially (e.g. if the tests are not thread-safe... in most cases, tests
# SHOULD be thread-safe). We also set a high-level timeout: if the test takes
# longer than the specified time, it is killed by the harness and reported as a
# failure. And finally, we provide a "label" that is used by CTest when
# reporting result statistics (e.g. "UnitTests: 72 successes, 3 failures").
# For more properties that can be set, see:
# https://cmake.org/cmake/help/v3.9/manual/cmake-properties.7.html#test-properties
set_tests_properties(${unit_gtests} PROPERTIES
        # RUN_SERIAL 1
        TIMEOUT 10 # seconds
        LABELS UnitTests
        )


add_custom_target(unit
    COMMAND ${CMAKE_BINARY_DIR}/tst/unit/unitTests
    DEPENDS unitTests 
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running unit tests..."
)