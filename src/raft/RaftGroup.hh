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
// Forward declarations
//------------------------------------------------------------------------------
class StateMachine; class RaftJournal; class RaftDispatcher;
class RaftState; class RaftReplicator; class RaftClock;
class RaftDirector; class RaftLease; class RaftWriteTracker;
class RaftTrimmer; class RaftCommitTracker; class RaftConfig;
class ShardDirectory;

//------------------------------------------------------------------------------
// This class keeps track of and owns all objects needed for the raft party.
// Everything is initialized lazily.
//------------------------------------------------------------------------------

class RaftGroup {
public:
  RaftGroup(ShardDirectory &shardDirectory, const RaftServer &myself, const RaftTimeouts &t);
  DISALLOW_COPY_AND_ASSIGN(RaftGroup);
  ~RaftGroup();

  StateMachine *stateMachine();
  RaftJournal *journal();
  RaftDispatcher *dispatcher();
  RaftState *state();
  RaftClock *raftclock();
  RaftDirector *director();
  RaftLease *lease();
  RaftCommitTracker *commitTracker();
  RaftWriteTracker *writeTracker();
  RaftTrimmer *trimmer();
  RaftConfig *config();
  RaftReplicator *replicator();

  RaftServer myself();

  void spinup();
  void spindown();
private:

  // Ownership managed external to this class.
  ShardDirectory &shardDirectory;
  StateMachine &stateMachineRef;
  RaftJournal &raftJournalRef;

  const RaftServer me;
  const RaftTimeouts timeouts;

  // All components needed for the raft party - owned by this class.
  RaftDispatcher *dispatcherptr = nullptr;
  RaftState *stateptr = nullptr;
  RaftClock *clockptr = nullptr;
  RaftDirector *directorptr = nullptr;
  RaftLease *leaseptr = nullptr;
  RaftCommitTracker *ctptr = nullptr;
  RaftWriteTracker *wtptr = nullptr;
  RaftTrimmer *trimmerptr = nullptr;
  RaftConfig *configptr = nullptr;
  RaftReplicator *replicatorptr = nullptr;
};

}

#endif
