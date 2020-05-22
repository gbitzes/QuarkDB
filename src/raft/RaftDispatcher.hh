// ----------------------------------------------------------------------
// File: RaftDispatcher.hh
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

#ifndef QUARKDB_RAFT_DISPATCHER_HH
#define QUARKDB_RAFT_DISPATCHER_HH

#include "Dispatcher.hh"
#include "pubsub/Publisher.hh"
#include "health/HealthIndicator.hh"
#include "raft/RaftUtils.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftBlockedWrites.hh"
#include <thread>
#include <chrono>

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class RaftJournal; class RaftState; class RaftHeartbeatTracker;
class RaftWriteTracker; class RaftReplicator; class Transaction;

struct RaftStateSnapshot;
using RaftStateSnapshotPtr = std::shared_ptr<const RaftStateSnapshot>;

class RaftDispatcher : public Dispatcher {
public:
  RaftDispatcher(RaftJournal &jour, StateMachine &sm, RaftState &st, RaftHeartbeatTracker &rht, RaftWriteTracker &rt, RaftReplicator &replicator, Publisher &publisher);
  DISALLOW_COPY_AND_ASSIGN(RaftDispatcher);

  LinkStatus dispatchInfo(Connection *conn, RedisRequest &req);
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;
  virtual LinkStatus dispatch(Connection *conn, Transaction &transaction) override final;
  virtual void notifyDisconnect(Connection *conn) override final;
  LinkStatus dispatchPubsub(Connection *conn, RedisRequest &req);

  RaftInfo info();
  bool fetch(LogIndex index, RaftEntry &entry);

  RaftHeartbeatResponse heartbeat(const RaftHeartbeatRequest &req);
  RaftAppendEntriesResponse appendEntries(RaftAppendEntriesRequest &&req);
  RaftVoteResponse requestVote(const RaftVoteRequest &req, bool preVote = false);

  //----------------------------------------------------------------------------
  // Return health information
  //----------------------------------------------------------------------------
  NodeHealth getHealth();

private:
  RaftHeartbeatResponse heartbeat(const RaftHeartbeatRequest &req, RaftStateSnapshotPtr &snapshot);
  LinkStatus service(Connection *conn, Transaction &tx);

  //----------------------------------------------------------------------------
  // Check if the removal of the given node would be acceptable
  //----------------------------------------------------------------------------
  bool checkIfNodeRemovalAcceptable(const RaftServer &srv);

  //----------------------------------------------------------------------------
  // Raft commands should not be run in parallel, but be serialized
  //----------------------------------------------------------------------------
  std::mutex raftCommand;

  //----------------------------------------------------------------------------
  // Injected dependencies
  //----------------------------------------------------------------------------
  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;
  RaftHeartbeatTracker &heartbeatTracker;
  RedisDispatcher redisDispatcher;
  RaftWriteTracker& writeTracker;
  RaftReplicator &replicator;
  Publisher &publisher;

  //----------------------------------------------------------------------------
  // Print a message when a follower is too far behind in regular intervals
  //----------------------------------------------------------------------------
  std::chrono::steady_clock::time_point lastLaggingWarning;
  void warnIfLagging(LogIndex leaderLogIndex);

};

}

#endif
