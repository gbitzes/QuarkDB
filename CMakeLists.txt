cmake_minimum_required(VERSION 2.8.11)

#-------------------------------------------------------------------------------
# Initialize
#-------------------------------------------------------------------------------

project(quarkdb)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(GNUInstallDirs)

#-------------------------------------------------------------------------------
# Regenerate Version.hh
#-------------------------------------------------------------------------------

add_custom_target(GenerateVersionInfo ALL DEPENDS Version)
add_custom_command(
  OUTPUT Version
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/genversion.py --template src/Version.hh.in --out src/Version.hh
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

#-------------------------------------------------------------------------------
# Look inside the rocksdb cache, which is used to greatly reduce compilation
# time of QuarkDB.
#
# To populate the /rocksdb cache, run:
# - sudo ./ci/install-rocksdb.sh
# - sudo CXXFLAGS='-fsanitize=thread' ./ci/install-rocksdb.sh
#
# The cache needs to be repopulated each time the rocksdb submodule changes
# to a different release.
#-------------------------------------------------------------------------------
option(BUILD_ROCKSDB "Build rocksdb, even if it exists system-wide or in the cache" OFF)

execute_process(
  COMMAND ./ci/canonical-rocksdb-path.sh
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE CANONICAL_ROCKSDB_PATH
)

set(ROCKSDB_ROOT_DIR ${CANONICAL_ROCKSDB_PATH} CACHE STRING "RocksDB cache dir")

if( (NOT BUILD_ROCKSDB) )
  # determine if we need to build rocksdb
  find_package(RocksDB)
  if( (NOT ROCKSDB_FOUND) )
    set(BUILD_ROCKSDB ON)
  endif()
endif()

if(BUILD_ROCKSDB)
  include(buildRocksDB)
  buildRocksDB()
endif()

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------

option(PACKAGEONLY "Build without dependencies" OFF)

if(NOT PACKAGEONLY)
  find_package(hiredis REQUIRED)
  find_package(XRootD REQUIRED)
  find_package(uuid REQUIRED)
endif()

#-------------------------------------------------------------------------------
# Check if we're generating a test coverage report
#-------------------------------------------------------------------------------
option(TESTCOVERAGE "Enable support for tracking test coverage" OFF)

if(TESTCOVERAGE)
  set(GCOV_CFLAGS "-fprofile-arcs -ftest-coverage --coverage")
  set(GCOV_LIBS "gcov")

  add_custom_target(
    raw-test-trace
    COMMAND lcov --capture --base-directory ${CMAKE_CURRENT_SOURCE_DIR} --directory ${CMAKE_BINARY_DIR} --output-file ${CMAKE_BINARY_DIR}/raw-trace.info
  )

  add_custom_target(
    filtered-test-trace
    COMMAND lcov --extract ${CMAKE_BINARY_DIR}/raw-trace.info "${CMAKE_CURRENT_SOURCE_DIR}/src/\\*" --output-file ${CMAKE_BINARY_DIR}/filtered-trace.info
    DEPENDS raw-test-trace
  )

  add_custom_target(
    coverage-report
    COMMAND genhtml ${CMAKE_BINARY_DIR}/filtered-trace.info --output-directory ${CMAKE_BINARY_DIR}/coverage-report
    DEPENDS filtered-test-trace
  )
endif()

#-------------------------------------------------------------------------------
# Use ccache, if available
#-------------------------------------------------------------------------------

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK "${CCACHE_PROGRAM}")
endif()

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------
add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -std=c++11 -g3 -fPIC)

#-------------------------------------------------------------------------------
# Build source and tests
#-------------------------------------------------------------------------------

if(NOT PACKAGEONLY)
  add_subdirectory(deps/backward-cpp)
  add_subdirectory(deps/qclient)
  add_subdirectory(test)
  add_subdirectory(src)
  add_subdirectory(tools)
endif()

#-------------------------------------------------------------------------------
# Packaging and source rpm
#-------------------------------------------------------------------------------

add_custom_target(
  dist
  COMMAND ./packaging/make-dist.sh
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_custom_target(
  srpm
  COMMAND ./packaging/make-srpm.sh
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
