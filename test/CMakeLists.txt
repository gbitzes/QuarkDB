cmake_minimum_required(VERSION 2.6)

#-------------------------------------------------------------------------------
# Set-up gtest
#-------------------------------------------------------------------------------

set(GTEST "${CMAKE_SOURCE_DIR}/deps/googletest/googletest")
set(GTEST_BINARY_DIR "${CMAKE_BINARY_DIR}/deps/googletest/googletest")
add_subdirectory("${GTEST}" "${GTEST_BINARY_DIR}")

#-------------------------------------------------------------------------------
# Build unit tests
#-------------------------------------------------------------------------------

file(GLOB_RECURSE sources "*.cc")
add_executable(tests ${sources})

include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/src  # need access to the private headers
)

target_link_libraries(tests
  XrdRedis
  gtest_main
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
)
