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

#include "Utils.hh"
#include "raft/RaftContactDetails.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class StateMachine; class RaftJournal; class RaftDispatcher;
class RaftState; class RaftReplicator; class RaftHeartbeatTracker;
class RaftDirector; class RaftLease; class RaftWriteTracker;
class RaftTrimmer; class RaftCommitTracker; class RaftConfig;
class ShardDirectory; class RaftContactDetails;
class Publisher;

//------------------------------------------------------------------------------
// This class keeps track of and owns all objects needed for the raft party.
// Everything is initialized lazily.
//------------------------------------------------------------------------------

class RaftGroup {
public:
  RaftGroup(ShardDirectory &shardDirectory, const RaftServer &myself, const RaftTimeouts &t, const std::string &password);
  DISALLOW_COPY_AND_ASSIGN(RaftGroup);
  ~RaftGroup();

  StateMachine *stateMachine();
  RaftJournal *journal();
  RaftDispatcher *dispatcher();
  RaftState *state();
  RaftHeartbeatTracker *heartbeatTracker();
  RaftDirector *director();
  RaftLease *lease();
  RaftCommitTracker *commitTracker();
  RaftWriteTracker *writeTracker();
  RaftTrimmer *trimmer();
  RaftConfig *config();
  RaftReplicator *replicator();
  Publisher *publisher();
  const RaftContactDetails* contactDetails() const;

  RaftServer myself();

  void spinup();
  void spindown();
private:
  std::recursive_mutex mtx;

  // Ownership managed external to this class.
  ShardDirectory &shardDirectory;
  StateMachine &stateMachineRef;
  RaftJournal &raftJournalRef;

  const RaftServer me;
  const RaftContactDetails raftContactDetails;

  // All components needed for the raft party - owned by this class.
  RaftDispatcher *dispatcherptr = nullptr;
  RaftState *stateptr = nullptr;
  RaftHeartbeatTracker *heartbeattrackerptr = nullptr;
  RaftDirector *directorptr = nullptr;
  RaftLease *leaseptr = nullptr;
  RaftCommitTracker *ctptr = nullptr;
  RaftWriteTracker *wtptr = nullptr;
  RaftTrimmer *trimmerptr = nullptr;
  RaftConfig *configptr = nullptr;
  RaftReplicator *replicatorptr = nullptr;
  Publisher *publisherptr = nullptr;
};

}

#endif
