cmake_minimum_required(VERSION 2.6)

include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${BACKWARD_INCLUDE_DIRS}
)

#-------------------------------------------------------------------------------
# XrdRedis plugin
#-------------------------------------------------------------------------------

add_library(XrdRedis SHARED
  XrdPlugin.cc
  XrdRedisProtocol.cc XrdRedisProtocol.hh
  Configuration.cc    Configuration.hh
                      RedisConnection.hh
  Utils.cc            Utils.hh
                      Common.hh
  Link.cc             Link.hh
  BufferedReader.cc   BufferedReader.hh
  ${BACKWARD_ENABLE}
)

target_link_libraries(
  XrdRedis
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${BACKWARD_LIBRARIES}
)
