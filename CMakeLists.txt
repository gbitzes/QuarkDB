cmake_minimum_required(VERSION 2.6)

#-------------------------------------------------------------------------------
# Initialize
#-------------------------------------------------------------------------------

project(quarkdb)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

#-------------------------------------------------------------------------------
# Regenerate Version.hh
#-------------------------------------------------------------------------------

add_custom_target(GenerateVersionInfo ALL DEPENDS Version)
add_custom_command(OUTPUT Version COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/genversion.py)

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------

find_package(XRootD REQUIRED)
find_package(RocksDB REQUIRED)
find_package(hiredis REQUIRED)

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------

add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -std=c++11 -g3 -fPIC)

#-------------------------------------------------------------------------------
# Build source and tests
#-------------------------------------------------------------------------------

add_subdirectory(deps/backward-cpp)
add_subdirectory(test)
add_subdirectory(src)
add_subdirectory(tools)
