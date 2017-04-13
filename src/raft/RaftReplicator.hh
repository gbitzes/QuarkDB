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

#include "../StateMachine.hh"
#include "RaftJournal.hh"
#include "RaftState.hh"
#include "RaftTimeouts.hh"
#include "RaftCommitTracker.hh"
#include "RaftLease.hh"
#include <mutex>

namespace quarkdb {

//------------------------------------------------------------------------------
// Tracks a single raft replica
//------------------------------------------------------------------------------
class RaftReplicaTracker {
public:
  RaftReplicaTracker(const RaftServer &target, const RaftStateSnapshot &snapshot, RaftJournal &journal, StateMachine &stateMachine, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, const RaftTimeouts t);
  ~RaftReplicaTracker();

  bool isRunning() { return running; }
private:
  void main();
  bool resilver();
  bool buildPayload(LogIndex nextIndex, int64_t payloadLimit,
    std::vector<RedisRequest> &reqs, std::vector<RaftTerm> &terms, int64_t &payloadSize);

  RaftServer target;
  RaftStateSnapshot snapshot;

  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  const RaftTimeouts timeouts;

  std::atomic<bool> running {false};
  std::atomic<bool> shutdown {false};
  std::thread thread;
};

//------------------------------------------------------------------------------
// A class that tracks multiple raft replicas over the duration of a single
// term
//------------------------------------------------------------------------------
class RaftReplicator {
public:
  RaftReplicator(RaftStateSnapshot &snapshot, RaftJournal &journal, StateMachine &stateMachine, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, const RaftTimeouts t);
  ~RaftReplicator();

  void setTargets(const std::vector<RaftServer> &targets);
private:
  RaftStateSnapshot snapshot;
  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  const RaftTimeouts timeouts;

  std::map<RaftServer, RaftReplicaTracker*> targets;
  std::mutex mtx;
};

}

#endif
