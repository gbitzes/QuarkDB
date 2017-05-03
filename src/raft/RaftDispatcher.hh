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
#include "RaftJournal.hh"
#include "RaftState.hh"
#include "RaftUtils.hh"
#include "RaftTimeouts.hh"
#include <thread>

namespace quarkdb {


class RaftDispatcher : public Dispatcher {
public:
  RaftDispatcher(RaftJournal &jour, StateMachine &sm, RaftState &st, RaftClock &rc);
  DISALLOW_COPY_AND_ASSIGN(RaftDispatcher);

  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;

  RaftInfo info();
  bool fetch(LogIndex index, RaftEntry &entry);
  bool checkpoint(const std::string &path, std::string &err);

  RaftAppendEntriesResponse appendEntries(RaftAppendEntriesRequest &&req);
  RaftVoteResponse requestVote(RaftVoteRequest &req);
  LinkStatus applyCommits(LogIndex index);
  void flushQueues(const std::string &msg);
private:
  LinkStatus service(Connection *conn, RedisRequest &req, RedisCommand &cmd, CommandType &type);
  LinkStatus applyOneCommit(LogIndex index);

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
  // The request queues live inside the connections, but we need to know *which*
  // connection is being blocked by *which* journal entry.
  //----------------------------------------------------------------------------
  std::map<LogIndex, std::shared_ptr<PendingQueue>> blockedWrites;

  //----------------------------------------------------------------------------
  // Misc
  //----------------------------------------------------------------------------
  RaftClock &raftClock;
  RedisDispatcher redisDispatcher;
};

}

#endif
