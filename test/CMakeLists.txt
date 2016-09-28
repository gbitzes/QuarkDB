cmake_minimum_required(VERSION 2.6)

set(GTEST_PATH deps/googletest/googletest)
set(GTEST ${CMAKE_SOURCE_DIR}/${GTEST_PATH})
set(GTEST_BINARY_DIR ${CMAKE_BINARY_DIR})

# Fetch all test cc files in all files and folders, recursively
file(GLOB_RECURSE sources "*.cc")
add_executable(tests ${sources})
include_directories(${GTEST}/include)

# The tests also need access to the private headers
include_directories(${CMAKE_SOURCE_DIR}/src)

# Build gtest
add_subdirectory(${GTEST} "${CMAKE_BINARY_DIR}/${GTEST_PATH}")

# Link
target_link_libraries(tests gtest_main)
