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

#ifndef __QUARKDB_RAFT_H__
#define __QUARKDB_RAFT_H__

#include "../Dispatcher.hh"
#include "RaftUtils.hh"
#include "RaftTimeouts.hh"
#include "RaftBlockedWrites.hh"
#include <thread>
#include <chrono>

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class RaftJournal; class RaftState; class RaftClock; class RaftWriteTracker;



class RaftDispatcher : public Dispatcher {
public:
  RaftDispatcher(RaftJournal &jour, StateMachine &sm, RaftState &st, RaftClock &rc, RaftWriteTracker &rt);
  DISALLOW_COPY_AND_ASSIGN(RaftDispatcher);

  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;

  RaftInfo info();
  bool fetch(LogIndex index, RaftEntry &entry);
  bool checkpoint(const std::string &path, std::string &err);

  RaftAppendEntriesResponse appendEntries(RaftAppendEntriesRequest &&req);
  RaftVoteResponse requestVote(RaftVoteRequest &req);
private:
  LinkStatus service(Connection *conn, RedisRequest &req, RedisCommand &cmd, CommandType &type);

  //----------------------------------------------------------------------------
  // Raft commands should not be run in parallel, but be serialized
  //----------------------------------------------------------------------------
  std::mutex raftCommand;

  //----------------------------------------------------------------------------
  // The all-important raft journal, state machine, and state tracker
  //----------------------------------------------------------------------------
  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;

  //----------------------------------------------------------------------------
  // Misc
  //----------------------------------------------------------------------------
  RaftClock &raftClock;
  RedisDispatcher redisDispatcher;
  RaftWriteTracker& writeTracker;

  //----------------------------------------------------------------------------
  // Print a message when a follower is too far behind in regular intervals
  //----------------------------------------------------------------------------
  std::chrono::steady_clock::time_point lastLaggingWarning;
  void warnIfLagging(LogIndex leaderLogIndex);

};

}

#endif
