cmake_minimum_required(VERSION 2.6)

include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${BACKWARD_INCLUDE_DIRS}
  ${ROCKSDB_INCLUDE_DIRS}
  ${HIREDIS_INCLUDE_DIRS}
)

#-------------------------------------------------------------------------------
# XrdRedis plugin
#-------------------------------------------------------------------------------

add_library(XrdRedis SHARED
  XrdPlugin.cc
  XrdRedisProtocol.cc      XrdRedisProtocol.hh
  Configuration.cc         Configuration.hh
                           RedisConnection.hh
  Utils.cc                 Utils.hh
                           Common.hh
  Link.cc                  Link.hh
  BufferedReader.cc        BufferedReader.hh
  RedisParser.cc           RedisParser.hh
  RocksDB.cc               RocksDB.hh
  Commands.cc              Commands.hh
  Response.cc              Response.hh
  Dispatcher.cc            Dispatcher.hh
  Tunnel.cc                Tunnel.hh
  Poller.cc                Poller.hh
  raft/RaftJournal.cc      raft/RaftJournal.hh
  raft/RaftState.cc        raft/RaftState.hh
  raft/RaftTalker.cc       raft/RaftTalker.hh
  raft/RaftParser.cc       raft/RaftParser.hh
  ${BACKWARD_ENABLE}
)

target_link_libraries(
  XrdRedis
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${ROCKSDB_LIBRARY}
  ${BACKWARD_LIBRARIES}
  ${HIREDIS_LIBRARIES}
)
