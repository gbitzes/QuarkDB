cmake_minimum_required(VERSION 2.6)
find_package (Threads)

add_executable(quarkdb-journal quarkdb-journal.cc)
target_link_libraries(quarkdb-journal XrdQuarkDB)

add_executable(quarkdb-scrub quarkdb-scrub.cc)
target_link_libraries(quarkdb-scrub XrdQuarkDB)

add_executable(quarkdb-create quarkdb-create.cc)
target_link_libraries(quarkdb-create XrdQuarkDB)

add_executable(quarkdb-chaos quarkdb-chaos.cc)
target_link_libraries(quarkdb-chaos XrdQuarkDB ${CMAKE_THREAD_LIBS_INIT})

include_directories(
  ${CMAKE_SOURCE_DIR}/src   # need access to the private headers
  ${CMAKE_SOURCE_DIR}/deps  # for optionparser.h
  ${ROCKSDB_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
)

install(
  TARGETS quarkdb-journal quarkdb-scrub quarkdb-chaos quarkdb-create
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
)
