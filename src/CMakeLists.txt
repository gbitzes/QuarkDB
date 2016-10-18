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
  XrdRedisProtocol.cc        XrdRedisProtocol.hh
  Configuration.cc           Configuration.hh
  Utils.cc                   Utils.hh
                             Common.hh
  Link.cc                    Link.hh
  BufferedReader.cc          BufferedReader.hh
  RedisParser.cc             RedisParser.hh
  RocksDB.cc                 RocksDB.hh
  Commands.cc                Commands.hh
  Response.cc                Response.hh
  Dispatcher.cc              Dispatcher.hh
  Tunnel.cc                  Tunnel.hh
  Poller.cc                  Poller.hh
  Connection.cc              Connection.hh
  raft/RaftJournal.cc        raft/RaftJournal.hh
  raft/RaftState.cc          raft/RaftState.hh
  raft/RaftTalker.cc         raft/RaftTalker.hh
  raft/RaftUtils.cc          raft/RaftUtils.hh
  raft/Raft.cc               raft/Raft.hh
  raft/RaftReplicator.cc     raft/RaftReplicator.hh
  raft/RaftTimeouts.cc       raft/RaftTimeouts.hh
  raft/RaftDirector.cc       raft/RaftDirector.hh
  raft/RaftCommitTracker.cc  raft/RaftCommitTracker.hh
  ${BACKWARD_ENABLE}
)

target_link_libraries(
  XrdRedis
  # libasan.a
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${ROCKSDB_LIBRARY}
  ${BACKWARD_LIBRARIES}
  ${HIREDIS_LIBRARIES}
)
