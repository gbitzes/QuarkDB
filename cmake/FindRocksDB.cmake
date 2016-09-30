# Try to find rocksdb
# Once done, this will define
#
# ROCKSDB_FOUND              - system has rocksdb
# ROCKSDB_INCLUDE_DIRS       - rocksdb include directories
# ROCKSDB_LIBRARY            - rocksdb library
#
# ROCKSDB_ROOT_DIR may be defined as a hint for where to look

include(FindPackageHandleStandardArgs)

if(ROCKSDB_INCLUDE_DIRS AND ROCKSDB_SHARED_LIBRARY)
  set(ROCKSDB_FIND_QUIETLY TRUE)
else()
  find_path(
    ROCKSDB_INCLUDE_DIRS
    NAMES rocksdb/version.h
    HINTS  ${ROCKSDB_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    ROCKSDB_LIBRARY
    NAMES librocksdb.a
    HINTS ${ROCKSDB_ROOT_DIR})

  find_package_handle_standard_args(
    rocksdb
    DEFAULT_MSG
    ROCKSDB_LIBRARY
    ROCKSDB_INCLUDE_DIRS)
endif()
