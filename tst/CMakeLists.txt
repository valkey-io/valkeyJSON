##################################################
# We use GoogleTest for both unit and system tests
##################################################
message("tst/CMakeLists.txt")

# Fetch GoogleTest.
include(FetchContent)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG 58d77fa8070e8cec2dc1ed015d66b454c8d78850 # release-1.12.1
  OVERRIDE_FIND_PACKAGE)
FetchContent_MakeAvailable(googletest)


include(GoogleTest)

add_subdirectory(unit)