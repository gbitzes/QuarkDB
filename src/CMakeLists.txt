include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${BACKWARD_INCLUDE_DIRS}
  ${ROCKSDB_INCLUDE_DIRS}
  ${HIREDIS_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
)

#-------------------------------------------------------------------------------
# xrootd plugin for QuarkDB plugin
#-------------------------------------------------------------------------------
add_definitions(${GCOV_CFLAGS})
add_library(XrdQuarkDB SHARED
  XrdPlugin.cc
  XrdQuarkDB.cc                    XrdQuarkDB.hh
  Configuration.cc                 Configuration.hh
  Utils.cc                         Utils.hh
                                   Common.hh
  Link.cc                          Link.hh
  BufferedReader.cc                BufferedReader.hh
  BufferedWriter.cc                BufferedWriter.hh
  RedisParser.cc                   RedisParser.hh
  RedisRequest.cc                  RedisRequest.hh
  StateMachine.cc                  StateMachine.hh
  Commands.cc                      Commands.hh
  Formatter.cc                     Formatter.hh
  Dispatcher.cc                    Dispatcher.hh
  Poller.cc                        Poller.hh
  Connection.cc                    Connection.hh
  QuarkDBNode.cc                   QuarkDBNode.hh
  Shard.cc                         Shard.hh
  ShardDirectory.cc                ShardDirectory.hh

  raft/RaftBlockedWrites.cc        raft/RaftBlockedWrites.hh
  raft/RaftConfig.cc               raft/RaftConfig.hh
  raft/RaftJournal.cc              raft/RaftJournal.hh
  raft/RaftState.cc                raft/RaftState.hh
  raft/RaftTalker.cc               raft/RaftTalker.hh
  raft/RaftUtils.cc                raft/RaftUtils.hh
  raft/RaftDispatcher.cc           raft/RaftDispatcher.hh
  raft/RaftReplicator.cc           raft/RaftReplicator.hh
  raft/RaftResilverer.cc           raft/RaftResilverer.hh
  raft/RaftTimeouts.cc             raft/RaftTimeouts.hh
  raft/RaftDirector.cc             raft/RaftDirector.hh
  raft/RaftCommitTracker.cc        raft/RaftCommitTracker.hh
  raft/RaftWriteTracker.cc         raft/RaftWriteTracker.hh
  raft/RaftGroup.cc                raft/RaftGroup.hh
                                   raft/RaftMembers.hh
  raft/RaftTrimmer.cc              raft/RaftTrimmer.hh
  raft/RaftLease.cc                raft/RaftLease.hh

  recovery/RecoveryDispatcher.cc   recovery/RecoveryDispatcher.hh
  recovery/RecoveryEditor.cc       recovery/RecoveryEditor.hh
  recovery/RecoveryRunner.cc       recovery/RecoveryRunner.hh

  redis/CommandMonitor.cc          redis/CommandMonitor.hh

  storage/ConsistencyScanner.cc    storage/ConsistencyScanner.hh
  storage/KeyConstants.cc          storage/KeyConstants.hh
                                   storage/KeyDescriptor.hh
  storage/KeyDescriptorBuilder.cc  storage/KeyDescriptorBuilder.hh
                                   storage/KeyLocators.hh
                                   storage/PatternMatching.hh
                                   storage/ReverseLocator.hh
                                   storage/StagingArea.hh

                                   utils/AssistedThread.hh
                                   utils/CommandParsing.hh
  utils/DirectoryIterator.cc       utils/DirectoryIterator.hh
  utils/FileUtils.cc               utils/FileUtils.hh
                                   utils/IntToBinaryString.hh
                                   utils/ParseUtils.hh
  utils/RequestCounter.cc          utils/RequestCounter.hh
  utils/Resilvering.cc             utils/Resilvering.hh
                                   utils/ScopedAdder.hh
                                   utils/StaticBuffer.hh
                                   utils/StringUtils.hh
  utils/TimeFormatting.cc          utils/TimeFormatting.hh
                                   utils/Uuid.hh
                                   utils/VectorUtils.hh

  ${BACKWARD_ENABLE}
)

target_link_libraries(
  XrdQuarkDB PUBLIC
  qclient
  bz2
  z
  rocksdb
  ssl
  jemalloc
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${BACKWARD_LIBRARIES}
  ${HIREDIS_LIBRARIES}
  ${UUID_LIBRARIES}
  ${GCOV_LIBS}
)

install(
  TARGETS XrdQuarkDB
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
