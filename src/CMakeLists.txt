include_directories(
  ${XROOTD_INCLUDE_DIRS}
  ${BACKWARD_INCLUDE_DIRS}
  ${ROCKSDB_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/deps/qclient/include
  "."
)

#-------------------------------------------------------------------------------
# xrootd plugin for QuarkDB plugin
#-------------------------------------------------------------------------------
add_definitions(${GCOV_CFLAGS})
add_library(XrdQuarkDB SHARED
  ../deps/xxhash/xxhash.cc                ../deps/xxhash/xxhash.hh

  XrdPlugin.cc
  XrdQuarkDB.cc                           XrdQuarkDB.hh
  Utils.cc                                Utils.hh
  BufferedReader.cc                       BufferedReader.hh
  BufferedWriter.cc                       BufferedWriter.hh
  Commands.cc                             Commands.hh
                                          Common.hh
  Configuration.cc                        Configuration.hh
  Connection.cc                           Connection.hh
  Dispatcher.cc                           Dispatcher.hh
                                          EventFD.hh
  Formatter.cc                            Formatter.hh
  Link.cc                                 Link.hh
  Poller.cc                               Poller.hh
  QuarkDBNode.cc                          QuarkDBNode.hh
  RedisParser.cc                          RedisParser.hh
  RedisRequest.cc                         RedisRequest.hh
  Shard.cc                                Shard.hh
  ShardDirectory.cc                       ShardDirectory.hh
  StandaloneGroup.cc                      StandaloneGroup.hh
  StateMachine.cc                         StateMachine.hh
  Timekeeper.cc                           Timekeeper.hh

  auth/AuthenticationDispatcher.cc        auth/AuthenticationDispatcher.hh
  config/ConfigurationReader.cc           config/ConfigurationReader.hh

                                          health/HealthIndicator.hh

                                          pubsub/SimplePatternMatcher.hh
  pubsub/Publisher.cc                     pubsub/Publisher.hh
  pubsub/SubscriptionTracker.cc           pubsub/SubscriptionTracker.hh

  raft/RaftBlockedWrites.cc               raft/RaftBlockedWrites.hh
  raft/RaftConfig.cc                      raft/RaftConfig.hh
  raft/RaftJournal.cc                     raft/RaftJournal.hh
  raft/RaftState.cc                       raft/RaftState.hh
  raft/RaftTalker.cc                      raft/RaftTalker.hh
  raft/RaftUtils.cc                       raft/RaftUtils.hh
  raft/RaftDispatcher.cc                  raft/RaftDispatcher.hh
  raft/RaftReplicator.cc                  raft/RaftReplicator.hh
  raft/RaftResilverer.cc                  raft/RaftResilverer.hh
  raft/RaftTimeouts.cc                    raft/RaftTimeouts.hh
  raft/RaftDirector.cc                    raft/RaftDirector.hh
  raft/RaftCommitTracker.cc               raft/RaftCommitTracker.hh
  raft/RaftWriteTracker.cc                raft/RaftWriteTracker.hh
  raft/RaftGroup.cc                       raft/RaftGroup.hh
                                          raft/RaftMembers.hh
  raft/RaftTrimmer.cc                     raft/RaftTrimmer.hh
  raft/RaftLease.cc                       raft/RaftLease.hh

  recovery/RecoveryDispatcher.cc          recovery/RecoveryDispatcher.hh
  recovery/RecoveryEditor.cc              recovery/RecoveryEditor.hh
  recovery/RecoveryRunner.cc              recovery/RecoveryRunner.hh

  redis/ArrayResponseBuilder.cc           redis/ArrayResponseBuilder.hh
  redis/Authenticator.cc                  redis/Authenticator.hh
  redis/InternalFilter.cc                 redis/InternalFilter.hh
  redis/CommandMonitor.cc                 redis/CommandMonitor.hh
  redis/LeaseFilter.cc                    redis/LeaseFilter.hh
  redis/MultiHandler.cc                   redis/MultiHandler.hh
                                          redis/RedisEncodedResponse.hh
  redis/Transaction.cc                    redis/Transaction.hh

  storage/ConsistencyScanner.cc           storage/ConsistencyScanner.hh
  storage/ExpirationEventIterator.cc      storage/ExpirationEventIterator.hh
  storage/InternalKeyParsing.cc           storage/InternalKeyParsing.hh
  storage/KeyConstants.cc                 storage/KeyConstants.hh
                                          storage/KeyDescriptor.hh
  storage/KeyDescriptorBuilder.cc         storage/KeyDescriptorBuilder.hh
                                          storage/KeyLocators.hh
                                          storage/LeaseInfo.hh
                                          storage/PatternMatching.hh
  storage/Randomization.cc                storage/Randomization.hh
                                          storage/ReverseLocator.hh
                                          storage/StagingArea.hh
  storage/VersionedHashRevisionTracker.cc storage/VersionedHashRevisionTracker.hh

                                          utils/AssistedThread.hh
                                          utils/CommandParsing.hh
  utils/DirectoryIterator.cc              utils/DirectoryIterator.hh
  utils/FileUtils.cc                      utils/FileUtils.hh
                                          utils/IntToBinaryString.hh
                                          utils/ParseUtils.hh
  utils/Random.cc                         utils/Random.hh
  utils/RequestCounter.cc                 utils/RequestCounter.hh
  utils/Resilvering.cc                    utils/Resilvering.hh
                                          utils/ScopedAdder.hh
                                          utils/StaticBuffer.hh
  utils/Statistics.cc                     utils/Statistics.hh
  utils/StringUtils.cc                    utils/StringUtils.hh
  utils/TimeFormatting.cc                 utils/TimeFormatting.hh
                                          utils/Uuid.hh
                                          utils/VectorUtils.hh

  ${BACKWARD_ENABLE}
)

if(BUILDING_WITH_TSAN)
    set(JEMALLOC_LIBS "")
else()
    set(JEMALLOC_LIBS jemalloc)
endif()

target_link_libraries(
  XrdQuarkDB PUBLIC
  qclient
  bz2
  z
  rocksdb
  ssl
  ${JEMALLOC_LIBS}
  lz4
  zstd
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${BACKWARD_LIBRARIES}
  ${UUID_LIBRARIES}
  ${GCOV_LIBS}
)

install(
  TARGETS XrdQuarkDB
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
