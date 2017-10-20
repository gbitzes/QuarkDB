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

#ifndef __QUARKDB_RAFT_REPLICATOR_H__
#define __QUARKDB_RAFT_REPLICATOR_H__

#include "RaftTimeouts.hh"
#include <mutex>
#include <queue>
#include "RaftTalker.hh"
#include "RaftState.hh"
#include "../utils/AssistedThread.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class RaftTalker; class RaftResilverer; class RaftTrimmer;
class ShardDirectory; class RaftConfig; class RaftState;
class StateMachine; class RaftJournal; class RaftLease;
class RaftCommitTracker; class RaftMatchIndexTracker; class RaftLastContact;

//------------------------------------------------------------------------------
// Tracks a single raft replica
//------------------------------------------------------------------------------

class RaftReplicaTracker {
public:
  RaftReplicaTracker(const RaftServer &target, const RaftStateSnapshot &snapshot, RaftJournal &journal, StateMachine &stateMachine, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, RaftTrimmer &trimmer, ShardDirectory &shardDirectory, RaftConfig &config, const RaftTimeouts t);
  ~RaftReplicaTracker();

  ReplicaStatus getStatus();
  bool isRunning() { return running; }
private:
  struct PendingResponse {
    PendingResponse(std::future<redisReplyPtr> f, std::chrono::steady_clock::time_point s, LogIndex pushed, int64_t payload)
    : fut(std::move(f)), sent(s), pushedFrom(pushed), payloadSize(payload) {}

    std::future<redisReplyPtr> fut;
    std::chrono::steady_clock::time_point sent;
    LogIndex pushedFrom;
    int64_t payloadSize;
  };

  void sendHeartbeats(ThreadAssistant &assistant);
  void main();
  LogIndex streamUpdates(RaftTalker &talker, LogIndex nextIndex);
  bool checkPendingQueue(std::queue<PendingResponse> &inflight);

  void triggerResilvering();
  bool buildPayload(LogIndex nextIndex, int64_t payloadLimit, std::vector<RaftEntry> &entries, int64_t &payloadSize);

  RaftServer target;
  RaftStateSnapshot snapshot;

  void updateStatus(bool online, LogIndex nextIndex);

  // Values to report when getStatus is called. Updated infrequently to avoid
  // overhead of atomics.
  std::atomic<bool> statusOnline {false};
  std::atomic<LogIndex> statusNextIndex {-1};

  ReplicaStatus currentStatus;

  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  RaftTrimmer &trimmer;
  ShardDirectory &shardDirectory;
  RaftConfig &config;
  const RaftTimeouts timeouts;

  RaftMatchIndexTracker &matchIndex;
  RaftLastContact &lastContact;

  std::atomic<bool> running {false};
  std::atomic<bool> shutdown {false};

  std::thread thread;
  AssistedThread heartbeatThread;

  RaftResilverer *resilverer = nullptr;
};


//------------------------------------------------------------------------------
// A class that tracks multiple raft replicas over the duration of a single
// term
//------------------------------------------------------------------------------

class RaftReplicator {
public:
  RaftReplicator(RaftJournal &journal, StateMachine &stateMachine, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, RaftTrimmer &trimmer, ShardDirectory &shardDirectory, RaftConfig &config, const RaftTimeouts t);
  ~RaftReplicator();

  void activate(RaftStateSnapshot &snapshot);
  void deactivate();

  ReplicationStatus getStatus();
  void reconfigure();
private:
  void setTargets(const std::vector<RaftServer> &targets);

  RaftStateSnapshot snapshot;
  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  RaftTrimmer &trimmer;
  ShardDirectory &shardDirectory;
  RaftConfig &config;
  const RaftTimeouts timeouts;

  std::map<RaftServer, RaftReplicaTracker*> targets;
  std::recursive_mutex mtx;

};

}

#endif
