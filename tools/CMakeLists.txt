cmake_minimum_required(VERSION 2.6)

add_executable(quarkdb-journal quarkdb-journal.cc)
target_link_libraries(quarkdb-journal XrdRedis)

add_executable(quarkdb-scrub quarkdb-scrub.cc)
target_link_libraries(quarkdb-scrub XrdRedis)

include_directories(
  ${CMAKE_SOURCE_DIR}/src   # need access to the private headers
  ${CMAKE_SOURCE_DIR}/deps  # for optionparser.h
  ${ROCKSDB_INCLUDE_DIRS}
)
