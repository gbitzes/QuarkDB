#-------------------------------------------------------------------------------
# Set-up gtest
#-------------------------------------------------------------------------------
set(GTEST "${CMAKE_SOURCE_DIR}/deps/googletest/googletest")
set(GTEST_BINARY_DIR "${CMAKE_BINARY_DIR}/deps/googletest/googletest")
add_subdirectory("${GTEST}" "${GTEST_BINARY_DIR}")

#-------------------------------------------------------------------------------
# Activate test coverage for these source files / binaries
#-------------------------------------------------------------------------------
add_definitions(${GCOV_CFLAGS})

#-------------------------------------------------------------------------------
# Source files shared across all test binaries
#-------------------------------------------------------------------------------
set(COMMON_TEST_SOURCES
  test-utils.cc                 test-utils.hh
  config/test-config.cc         config/test-config.hh
)

#-------------------------------------------------------------------------------
# Build unit tests
#-------------------------------------------------------------------------------
add_executable(quarkdb-tests
  buffered-reader.cc
  configuration.cc
  connection.cc
  dispatcher.cc
  e2e.cc
  formatter.cc
  link.cc
  poller.cc
  raft-journal.cc
  raft-lease.cc
  raft-parser.cc
  raft-state.cc
  raft-talker.cc
  raft.cc
  redis-parser.cc
  state-machine.cc
  ${COMMON_TEST_SOURCES}
  tunnel.cc
  utils.cc
)

#-------------------------------------------------------------------------------
# Build bench tool
#-------------------------------------------------------------------------------
add_executable(quarkdb-bench
  bench/hset.cc
  bench/main.cc
  ${COMMON_TEST_SOURCES}
)

#-------------------------------------------------------------------------------
# Build bench-malloc tool
#-------------------------------------------------------------------------------
add_executable(quarkdb-bench-malloc
  bench-malloc/interceptor.cc
  bench-malloc/main.cc
  bench-malloc/measure.cc
  ${COMMON_TEST_SOURCES}
)

#-------------------------------------------------------------------------------
# Build stress tool
#-------------------------------------------------------------------------------
add_executable(quarkdb-stress-tests
  stress/background-flusher.cc
  stress/bulkload.cc
  stress/main.cc
  stress/replication.cc
  stress/resilvering.cc
  ${COMMON_TEST_SOURCES}
)

include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/src  # need access to the private headers
  ${ROCKSDB_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
)

#-------------------------------------------------------------------------------
# Link
#-------------------------------------------------------------------------------
target_link_libraries(quarkdb-tests
  XrdQuarkDB
  gtest_main
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${GCOV_LIBS}
)

target_link_libraries(quarkdb-bench
  XrdQuarkDB
  gtest_main
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${GCOV_LIBS}
)

target_link_libraries(quarkdb-bench-malloc
  XrdQuarkDB
  gtest_main
  dl
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${GCOV_LIBS}
)

target_link_libraries(quarkdb-stress-tests
  XrdQuarkDB
  gtest_main
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${GCOV_LIBS}
)

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS quarkdb-tests quarkdb-bench quarkdb-stress-tests
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
)
