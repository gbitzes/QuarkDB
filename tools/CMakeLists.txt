cmake_minimum_required(VERSION 2.6)

add_executable(quarkdb-journal quarkdb-journal.cc)

include_directories(
  ${CMAKE_SOURCE_DIR}/src   # need access to the private headers
  ${CMAKE_SOURCE_DIR}/deps  # for optionparser.h
  ${ROCKSDB_INCLUDE_DIRS}
)


target_link_libraries(quarkdb-journal
  XrdRedis
)
