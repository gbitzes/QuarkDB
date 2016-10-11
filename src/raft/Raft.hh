// ----------------------------------------------------------------------
// File: Raft.hh
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

class Raft : public Dispatcher {
public:
  Raft(RaftJournal &jour, RocksDB &sm, RaftState &st, RaftClock &rc);
  DISALLOW_COPY_AND_ASSIGN(Raft);

  virtual LinkStatus dispatch(Link *link, RedisRequest &req) override;

  RaftInfo info();
  bool fetch(LogIndex index, RaftEntry &entry);

  RaftAppendEntriesResponse appendEntries(RaftAppendEntriesRequest &&req);
  RaftVoteResponse requestVote(RaftVoteRequest &req);

  //----------------------------------------------------------------------------
  // Only used for testing
  //----------------------------------------------------------------------------
  RaftState *getState();
private:
  LinkStatus service(Link *link, RedisRequest &req, RedisCommand &cmd, CommandType &type);

  //----------------------------------------------------------------------------
  // Raft commands should not be run in parallel, but be serialized
  //----------------------------------------------------------------------------
  std::mutex raftCommand;

  //----------------------------------------------------------------------------
  // The all-important raft journal, state machine, and state tracker
  //----------------------------------------------------------------------------
  RaftJournal &journal;
  RocksDB &stateMachine;
  RaftState &state;

  //----------------------------------------------------------------------------
  // Misc
  //----------------------------------------------------------------------------
  RaftClock &raftClock;

};

}

#endif
