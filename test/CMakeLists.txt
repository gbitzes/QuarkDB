#-------------------------------------------------------------------------------
# Set-up gtest
#-------------------------------------------------------------------------------
set(GTEST "${CMAKE_SOURCE_DIR}/deps/googletest/googletest")
set(GTEST_BINARY_DIR "${CMAKE_BINARY_DIR}/deps/googletest/googletest")
add_subdirectory("${GTEST}" "${GTEST_BINARY_DIR}")

#-------------------------------------------------------------------------------
# Build unit tests
#-------------------------------------------------------------------------------
add_executable(tests
  buffered-reader.cc
  configuration.cc
  connection.cc
  dispatcher.cc
  e2e.cc
  link.cc
  poller.cc
  raft-journal.cc
  raft-parser.cc
  raft-state.cc
  raft-talker.cc
  raft.cc
  redis-parser.cc
  formatter.cc
  state-machine.cc
  test-utils.cc                 test-utils.hh
  tunnel.cc
  utils.cc
)

#-------------------------------------------------------------------------------
# Build bench tool
#-------------------------------------------------------------------------------
add_executable(bench
  bench/hset.cc
  test-utils.cc                 test-utils.hh
)

include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/src  # need access to the private headers
  ${ROCKSDB_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
)

target_link_libraries(tests
  XrdRedis
  gtest_main
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
)

target_link_libraries(bench
  XrdRedis
  gtest_main
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
)
