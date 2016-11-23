macro(buildRocksDB)
  include(ExternalProject)

  ExternalProject_Add(RocksDB
    URL "${CMAKE_SOURCE_DIR}/deps/rocksdb"
    PREFIX "${CMAKE_BINARY_DIR}/deps/rocksdb"
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND bash -c "export PORTABLE=1 && export DISABLE_JEMALLOC=1 && export OPT='-fPIC -DNDEBUG -O3' && make static_lib"
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
    LOG_BUILD ON
  )

  ExternalProject_Get_Property(RocksDB source_dir)
  set(ROCKSDB_INCLUDE_DIRS ${source_dir}/include)
  include_directories(${ROCKSDB_INCLUDE_DIRS})
  ExternalProject_Get_Property(RocksDB binary_dir)
  set(ROCKSDB_LIBRARY ${binary_dir}/librocksdb.a)
endmacro()
