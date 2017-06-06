// ----------------------------------------------------------------------
// File: RaftDirector.hh
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

#ifndef __QUARKDB_RAFT_DIRECTOR_H__
#define __QUARKDB_RAFT_DIRECTOR_H__

#include "../StateMachine.hh"
#include "RaftJournal.hh"
#include "RaftState.hh"
#include "RaftTimeouts.hh"
#include "RaftDispatcher.hh"
#include "RaftLease.hh"
#include "RaftCommitTracker.hh"
#include "RaftWriteTracker.hh"
#include <thread>

namespace quarkdb {

class RaftDirector {
public:
  RaftDirector(RaftDispatcher &disp, RaftJournal &journal, StateMachine &stateMachine, RaftState &state, RaftLease &lease, RaftCommitTracker &commitTracker, RaftClock &rc, RaftWriteTracker &wt);
  ~RaftDirector();
  DISALLOW_COPY_AND_ASSIGN(RaftDirector);
private:
  void main();
  void actAsFollower(RaftStateSnapshot &snapshot);
  void actAsLeader(RaftStateSnapshot &snapshot);
  void runForLeader();
  void applyCommits();
  void trimJournal();

  RaftDispatcher &dispatcher;
  RaftJournal &journal;
  StateMachine &stateMachine;
  RaftState &state;
  RaftClock &raftClock;
  RaftLease &lease;
  RaftCommitTracker &commitTracker;
  RaftWriteTracker &writeTracker;

  std::thread mainThread;
  std::thread commitApplier;
  std::thread journalTrimmer;

  std::atomic<bool> commitApplierActive {true};
};

}

#endif
