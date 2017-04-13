// ----------------------------------------------------------------------
// File: RaftGroup.hh
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

#ifndef __QUARKDB_RAFT_GROUP_H__
#define __QUARKDB_RAFT_GROUP_H__

#include "../Utils.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// This class keeps track of and owns all objects needed for the raft party.
// Everything is initialized lazily.
//
// The journal and rocksdb objects can be injected, and in that case are not
// owned by us.
//------------------------------------------------------------------------------

class StateMachine; class RaftJournal; class RaftDispatcher;
class RaftState; class RaftReplicator; class RaftClock;
class RaftDirector; class RaftLease;

class RaftGroup {
public:
  RaftGroup(const std::string &path, const RaftServer &myself, const RaftTimeouts &t);
  RaftGroup(RaftJournal &journal, StateMachine &stateMachine, const RaftServer &myself, const RaftTimeouts &t);
  DISALLOW_COPY_AND_ASSIGN(RaftGroup);
  ~RaftGroup();

  StateMachine *stateMachine();
  RaftJournal *journal();
  RaftDispatcher *dispatcher();
  RaftState *state();
  RaftReplicator *replicator();
  RaftClock *raftclock();
  RaftDirector *director();
  RaftLease *lease();
  RaftCommitTracker *commitTracker();

  RaftServer myself();

  void spinup();
  void spindown();
private:
  bool injectedDatabases = false;
  const RaftServer me;
  const RaftTimeouts timeouts;

  StateMachine *smptr = nullptr;
  RaftJournal *journalptr = nullptr;
  RaftDispatcher *dispatcherptr = nullptr;
  RaftReplicator *replicatorptr = nullptr;
  RaftState *stateptr = nullptr;
  RaftClock *clockptr = nullptr;
  RaftDirector *directorptr = nullptr;
  RaftLease *leaseptr = nullptr;
  RaftCommitTracker *ctptr = nullptr;
};

}

#endif
