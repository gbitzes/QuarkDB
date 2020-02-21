cmake_minimum_required(VERSION 2.6)
find_package (Threads)

add_executable(quarkdb-create quarkdb-create.cc)
target_link_libraries(quarkdb-create XrdQuarkDB)

add_executable(quarkdb-ldb quarkdb-ldb.cc)
target_link_libraries(quarkdb-ldb XrdQuarkDB ${CMAKE_THREAD_LIBS_INIT})

add_executable(quarkdb-recovery quarkdb-recovery.cc)
target_link_libraries(quarkdb-recovery XrdQuarkDB ${CMAKE_THREAD_LIBS_INIT} )

add_executable(quarkdb-server quarkdb-server.cc)
target_link_libraries(quarkdb-server jemalloc XrdQuarkDB ${CMAKE_THREAD_LIBS_INIT} )

add_executable(quarkdb-sst-inspect quarkdb-sst-inspect.cc)
target_link_libraries(quarkdb-sst-inspect XrdQuarkDB ${CMAKE_THREAD_LIBS_INIT} )

add_executable(quarkdb-validate-checkpoint quarkdb-validate-checkpoint.cc)
target_link_libraries(quarkdb-validate-checkpoint XrdQuarkDB ${CMAKE_THREAD_LIBS_INIT} )

include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/src   # need access to the private headers
  ${CMAKE_SOURCE_DIR}/deps  # for optionparser.h
  ${ROCKSDB_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
  ${CMAKE_SOURCE_DIR}/deps/asio/asio/include
  ${CMAKE_SOURCE_DIR}/deps/CLI11/include
)

install(
  TARGETS quarkdb-create quarkdb-ldb quarkdb-recovery quarkdb-server quarkdb-sst-inspect quarkdb-validate-checkpoint
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
)
