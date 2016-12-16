include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
)

#-------------------------------------------------------------------------------
# XrdQuarkOfs plugin
#-------------------------------------------------------------------------------
add_library(
  XrdQuarkOfs MODULE
  XrdQuarkOfs.cc XrdQuarkOfs.hh)

target_link_libraries(
  XrdQuarkOfs
  XrdRedis
  ${XROOTD_SERVER_LIBRARY})

install(
  TARGETS
  XrdQuarkOfs
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
