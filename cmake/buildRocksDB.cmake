macro(buildRocksDB)
  include(ExternalProject)
  include(CheckCXXCompilerFlag)

  check_cxx_compiler_flag(-Wno-error=deprecated-copy COMPILER_WARNS_DEPRECATED_COPY)
  check_cxx_compiler_flag(-Wno-error=pessimizing-move COMPILER_WARNS_PESSIMIZING_MOVE)

  set(EXTRA_OPTIONS " ")

  if(COMPILER_WARNS_DEPRECATED_COPY)
    set(EXTRA_OPTIONS " ${EXTRA_OPTIONS} -Wno-error=deprecated-copy")
  endif()

  if(COMPILER_WARNS_PESSIMIZING_MOVE)
    set(EXTRA_OPTIONS " ${EXTRA_OPTIONS} -Wno-error=pessimizing-move")
  endif()

  ExternalProject_Add(BuildRocksDB
    URL "${CMAKE_SOURCE_DIR}/deps/rocksdb"
    PREFIX "${CMAKE_BINARY_DIR}/deps/rocksdb"
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE 1
    BUILD_COMMAND bash -c "export PORTABLE=1 && export DISABLE_JEMALLOC=1 && export OPT='-fPIC -DNDEBUG -O3 ${EXTRA_OPTIONS} ' && export USE_RTTI=1 && make static_lib -j $(cat /proc/cpuinfo | grep 'processor' | wc -l)"
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(BuildRocksDB source_dir)
  set(ROCKSDB_INCLUDE_DIRS ${source_dir}/include)
  ExternalProject_Get_Property(BuildRocksDB binary_dir)

  add_library(rocksdb STATIC IMPORTED)
  set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${binary_dir}/librocksdb.a)
  add_dependencies(rocksdb BuildRocksDB)
endmacro()
