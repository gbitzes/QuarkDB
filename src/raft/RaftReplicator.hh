// ----------------------------------------------------------------------
// File: RaftReplicator.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2016 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef QUARKDB_RAFT_REPLICATOR_H
#define QUARKDB_RAFT_REPLICATOR_H

#include "RaftTimeouts.hh"
#include <mutex>
#include <queue>
#include "raft/RaftTalker.hh"
#include "raft/RaftState.hh"
#include "raft/RaftTrimmer.hh"
#include "utils/AssistedThread.hh"
#include "utils/ThreadSafeString.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class RaftTalker; class RaftResilverer; class RaftTrimmer;
class ShardDirectory; class RaftConfig; class RaftState;
class StateMachine; class RaftJournal; class RaftLease;
class RaftCommitTracker; class RaftMatchIndexTracker; class RaftLastContact;
class RaftContactDetails;

//------------------------------------------------------------------------------
// Tracks a single raft replica
//------------------------------------------------------------------------------

class RaftReplicaTracker {
public:
  RaftReplicaTracker(const RaftServer &target, const RaftStateSnapshotPtr &snapshot, RaftJournal &journal, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, RaftTrimmer &trimmer, ShardDirectory &shardDirectory, RaftConfig &config, const RaftContactDetails &contactDetails);
  ~RaftReplicaTracker();

  ReplicaStatus getStatus();
  bool isRunning() { return running; }
private:
  struct PendingResponse {
    PendingResponse(std::future<redisReplyPtr> &&f, std::chrono::steady_clock::time_point s, LogIndex pushed, int64_t payload, RaftTerm let)
    : fut(std::move(f)), sent(s), pushedFrom(pushed), payloadSize(payload), lastEntryTerm(let) {}

    std::future<redisReplyPtr> fut;
    std::chrono::steady_clock::time_point sent;
    LogIndex pushedFrom;
    int64_t payloadSize;
    RaftTerm lastEntryTerm;
  };

  void sendHeartbeats(ThreadAssistant &assistant);
  void main();

  void monitorAckReception(ThreadAssistant &assistant);
  std::mutex inFlightMtx;
  std::condition_variable inFlightCV;
  std::condition_variable inFlightPoppedCV;

  std::queue<PendingResponse> inFlight;
  std::atomic<bool> streamingUpdates;

  LogIndex streamUpdates(RaftTalker &talker, LogIndex nextIndex);

  void triggerResilvering();
  bool buildPayload(LogIndex nextIndex, int64_t payloadLimit, std::vector<RaftSerializedEntry> &entries,
    int64_t &payloadSize, RaftTerm &lastEntryTerm);

  bool sendPayload(RaftTalker &talker, LogIndex nextIndex, int64_t payloadLimit,
    std::future<redisReplyPtr> &reply, std::chrono::steady_clock::time_point &contact, int64_t &payloadSize,
    RaftTerm &lastEntryTerm);

  RaftServer target;
  RaftStateSnapshotPtr snapshot;

  void updateStatus(bool online, LogIndex nextIndex);

  // Values to report when getStatus is called. Updated infrequently to avoid
  // overhead of atomics.
  std::atomic<bool> statusOnline {false};
  std::atomic<LogIndex> statusNextIndex {-1};
  ThreadSafeString statusNodeVersion {"N/A"};

  RaftJournal &journal;
  RaftState &state;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  RaftTrimmer &trimmer;
  ShardDirectory &shardDirectory;
  RaftConfig &config;
  const RaftContactDetails &contactDetails;

  RaftMatchIndexTracker &matchIndex;
  RaftLastContact &lastContact;

  std::atomic<bool> running {false};
  std::atomic<bool> shutdown {false};

  std::thread thread;
  AssistedThread heartbeatThread;

  std::unique_ptr<RaftResilverer> resilverer;
  RaftTrimmingBlock trimmingBlock;
};


//------------------------------------------------------------------------------
// A class that tracks multiple raft replicas over the duration of a single
// term
//------------------------------------------------------------------------------

class RaftReplicator {
public:
  RaftReplicator(RaftJournal &journal, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, RaftTrimmer &trimmer, ShardDirectory &shardDirectory, RaftConfig &config, const RaftContactDetails &contactDetails);
  ~RaftReplicator();

  void activate(RaftStateSnapshotPtr &snapshot);
  void deactivate();

  ReplicationStatus getStatus();
  void reconfigure();
private:
  void setTargets(const std::vector<RaftServer> &targets);

  RaftStateSnapshotPtr snapshot;
  RaftJournal &journal;
  RaftState &state;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  RaftTrimmer &trimmer;
  ShardDirectory &shardDirectory;
  RaftConfig &config;
  const RaftContactDetails &contactDetails;

  std::map<RaftServer, RaftReplicaTracker*> targets;
  std::recursive_mutex mtx;

};

}

#endif
