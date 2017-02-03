macro(buildRocksDB)
  include(ExternalProject)

  ExternalProject_Add(BuildRocksDB
    URL "${CMAKE_SOURCE_DIR}/deps/rocksdb"
    PREFIX "${CMAKE_BINARY_DIR}/deps/rocksdb"
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND bash -c "export PORTABLE=1 && export DISABLE_JEMALLOC=1 && export OPT='-fPIC -DNDEBUG -O3' && make static_lib"
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(BuildRocksDB source_dir)
  set(ROCKSDB_INCLUDE_DIRS ${source_dir}/include)
  ExternalProject_Get_Property(BuildRocksDB binary_dir)

  add_library(rocksdb STATIC IMPORTED)
  set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${binary_dir}/librocksdb.a)
  add_dependencies(rocksdb BuildRocksDB)
endmacro()
