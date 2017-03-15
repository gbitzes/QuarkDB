include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${BACKWARD_INCLUDE_DIRS}
  ${ROCKSDB_INCLUDE_DIRS}
  ${HIREDIS_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
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
  BufferedWriter.cc          BufferedWriter.hh
  RedisParser.cc             RedisParser.hh
  StateMachine.cc            StateMachine.hh
  Commands.cc                Commands.hh
  Formatter.cc               Formatter.hh
  Dispatcher.cc              Dispatcher.hh
  Poller.cc                  Poller.hh
  Connection.cc              Connection.hh
  QuarkDBNode.cc             QuarkDBNode.hh
  raft/RaftJournal.cc        raft/RaftJournal.hh
  raft/RaftState.cc          raft/RaftState.hh
  raft/RaftTalker.cc         raft/RaftTalker.hh
  raft/RaftUtils.cc          raft/RaftUtils.hh
  raft/RaftDispatcher.cc     raft/RaftDispatcher.hh
  raft/RaftReplicator.cc     raft/RaftReplicator.hh
  raft/RaftTimeouts.cc       raft/RaftTimeouts.hh
  raft/RaftDirector.cc       raft/RaftDirector.hh
  raft/RaftCommitTracker.cc  raft/RaftCommitTracker.hh
  raft/RaftGroup.cc          raft/RaftGroup.hh
                             raft/RaftMembers.hh
  ${BACKWARD_ENABLE}
)

target_link_libraries(
  XrdRedis PUBLIC
  qclient
  bz2
  z
  rocksdb
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${BACKWARD_LIBRARIES}
  ${HIREDIS_LIBRARIES}
)

install(
  TARGETS XrdRedis
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
