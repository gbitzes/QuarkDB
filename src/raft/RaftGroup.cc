// ----------------------------------------------------------------------
// File: RaftGroup.cc
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

#include "raft/RaftJournal.hh"
#include "raft/RaftDispatcher.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftDirector.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftLease.hh"
#include "StateMachine.hh"
#include "raft/RaftGroup.hh"
#include "raft/RaftWriteTracker.hh"
#include "raft/RaftTrimmer.hh"
#include "raft/RaftConfig.hh"
#include "utils/FileUtils.hh"
#include "ShardDirectory.hh"

using namespace quarkdb;

RaftGroup::RaftGroup(ShardDirectory &shardDir, const RaftServer &myself, const RaftTimeouts &t, const std::string &password)
: shardDirectory(shardDir), stateMachineRef(*shardDirectory.getStateMachine()),
  raftJournalRef(*shardDirectory.getRaftJournal()), me(myself),
  raftContactDetails(raftJournalRef.getClusterID(), t, password) {

}

RaftGroup::~RaftGroup() {
  spindown();
}

void RaftGroup::spinup() {
  std::scoped_lock lock(mtx);
  trimmer();
  director(); // transitively initializes everything else
}

void RaftGroup::spindown() {
  std::scoped_lock lock(mtx);
  // Delete everything except the journal and store
  if(directorptr) {
    delete directorptr;
    directorptr = nullptr;
  }
  if(dispatcherptr) {
    delete dispatcherptr;
    dispatcherptr = nullptr;
  }
  if(replicatorptr) {
    delete replicatorptr;
    replicatorptr = nullptr;
  }
  if(trimmerptr) {
    delete trimmerptr;
    trimmerptr = nullptr;
  }
  if(configptr) {
    delete configptr;
    configptr = nullptr;
  }
  if(wtptr) {
    delete wtptr;
    wtptr = nullptr;
  }
  if(stateptr) {
    delete stateptr;
    stateptr = nullptr;
  }
  if(heartbeattrackerptr) {
    delete heartbeattrackerptr;
    heartbeattrackerptr = nullptr;
  }
  if(leaseptr) {
    delete leaseptr;
    leaseptr = nullptr;
  }
  if(ctptr) {
    delete ctptr;
    ctptr = nullptr;
  }

  if(publisherptr) {
    delete publisherptr;
    publisherptr = nullptr;
  }
}

RaftServer RaftGroup::myself() {
  return me;
}

StateMachine* RaftGroup::stateMachine() {
  // always available
  return &stateMachineRef;
}

RaftJournal* RaftGroup::journal() {
  // always available
  return &raftJournalRef;
}

RaftDispatcher* RaftGroup::dispatcher() {
  std::scoped_lock lock(mtx);
  if(dispatcherptr == nullptr) {
    dispatcherptr = new RaftDispatcher(*journal(), *stateMachine(), *state(), *heartbeatTracker(), *writeTracker(), *replicator(), *publisher());
  }
  return dispatcherptr;
}

RaftHeartbeatTracker* RaftGroup::heartbeatTracker() {
  std::scoped_lock lock(mtx);
  if(heartbeattrackerptr == nullptr) {
    heartbeattrackerptr = new RaftHeartbeatTracker(contactDetails()->getRaftTimeouts());
  }
  return heartbeattrackerptr;
}

RaftState* RaftGroup::state() {
  std::scoped_lock lock(mtx);
  if(stateptr == nullptr) {
    stateptr = new RaftState(*journal(), myself());
  }
  return stateptr;
}

RaftDirector* RaftGroup::director() {
  std::scoped_lock lock(mtx);
  if(directorptr == nullptr) {
    directorptr = new RaftDirector(*journal(), *stateMachine(), *state(), *lease(), *commitTracker(), *heartbeatTracker(), *writeTracker(), shardDirectory, *config(), *replicator(), *contactDetails(), *publisher());
  }
  return directorptr;
}

RaftLease* RaftGroup::lease() {
  std::scoped_lock lock(mtx);
  if(leaseptr == nullptr) {
    leaseptr = new RaftLease(journal()->getMembership().nodes, heartbeatTracker()->getTimeouts().getLow());
  }
  return leaseptr;
}

RaftCommitTracker* RaftGroup::commitTracker() {
  std::scoped_lock lock(mtx);
  if(ctptr == nullptr) {
    ctptr = new RaftCommitTracker(*journal());
  }
  return ctptr;
}

RaftWriteTracker* RaftGroup::writeTracker() {
  std::scoped_lock lock(mtx);
  if(wtptr == nullptr) {
    wtptr = new RaftWriteTracker(*journal(), *stateMachine(), *publisher());
  }
  return wtptr;
}

RaftTrimmer* RaftGroup::trimmer() {
  std::scoped_lock lock(mtx);
  if(trimmerptr == nullptr) {
    trimmerptr = new RaftTrimmer(*journal(), *config(), *stateMachine());
  }
  return trimmerptr;
}

RaftConfig* RaftGroup::config() {
  std::scoped_lock lock(mtx);
  if(configptr == nullptr) {
    configptr = new RaftConfig(*stateMachine());
  }
  return configptr;
}

RaftReplicator* RaftGroup::replicator() {
  std::scoped_lock lock(mtx);
  if(replicatorptr == nullptr) {
    replicatorptr = new RaftReplicator(*journal(), *state(), *lease(), *commitTracker(), *trimmer(), shardDirectory, *config(), *contactDetails());
  }
  return replicatorptr;
}

Publisher* RaftGroup::publisher() {
  std::scoped_lock lock(mtx);
  if(publisherptr == nullptr) {
    publisherptr = new Publisher();
  }
  return publisherptr;
}

const RaftContactDetails* RaftGroup::contactDetails() const {
  return &raftContactDetails;
}
