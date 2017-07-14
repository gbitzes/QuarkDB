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

#include "RaftJournal.hh"
#include "RaftDispatcher.hh"
#include "RaftTimeouts.hh"
#include "RaftDirector.hh"
#include "RaftReplicator.hh"
#include "RaftLease.hh"
#include "../StateMachine.hh"
#include "RaftGroup.hh"
#include "RaftWriteTracker.hh"
#include "RaftTrimmer.hh"
#include "RaftConfig.hh"
#include "../utils/FileUtils.hh"
#include "../ShardDirectory.hh"

using namespace quarkdb;

RaftGroup::RaftGroup(ShardDirectory &shardDir, const RaftServer &myself, const RaftTimeouts &t)
: shardDirectory(shardDir), stateMachineRef(*shardDirectory.getStateMachine()),
raftJournalRef(*shardDirectory.getRaftJournal()), me(myself), timeouts(t) {

}

RaftGroup::~RaftGroup() {
  spindown();
}

void RaftGroup::spinup() {
  trimmer();
  director(); // transitively initializes everything else
}

void RaftGroup::spindown() {
  // Delete everything except the journal and store
  if(directorptr) {
    delete directorptr;
    directorptr = nullptr;
  }
  if(trimmerptr) {
    delete trimmerptr;
    trimmerptr = nullptr;
  }
  if(configptr) {
    delete configptr;
    configptr = nullptr;
  }
  if(dispatcherptr) {
    delete dispatcherptr;
    dispatcherptr = nullptr;
  }
  if(wtptr) {
    delete wtptr;
    wtptr = nullptr;
  }
  if(stateptr) {
    delete stateptr;
    stateptr = nullptr;
  }
  if(clockptr) {
    delete clockptr;
    clockptr = nullptr;
  }
  if(leaseptr) {
    delete leaseptr;
    leaseptr = nullptr;
  }
  if(ctptr) {
    delete ctptr;
    ctptr = nullptr;
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
  if(dispatcherptr == nullptr) {
    dispatcherptr = new RaftDispatcher(*journal(), *stateMachine(), *state(), *raftclock(), *writeTracker());
  }
  return dispatcherptr;
}

RaftClock* RaftGroup::raftclock() {
  if(clockptr == nullptr) {
    clockptr = new RaftClock(timeouts);
  }
  return clockptr;
}

RaftState* RaftGroup::state() {
  if(stateptr == nullptr) {
    stateptr = new RaftState(*journal(), myself());
  }
  return stateptr;
}

RaftDirector* RaftGroup::director() {
  if(directorptr == nullptr) {
    directorptr = new RaftDirector(*dispatcher(), *journal(), *stateMachine(), *state(), *lease(), *commitTracker(), *raftclock(), *writeTracker());
  }
  return directorptr;
}

RaftLease* RaftGroup::lease() {
  if(leaseptr == nullptr) {
    leaseptr = new RaftLease(journal()->getMembership().nodes, raftclock()->getTimeouts().getLow());
  }
  return leaseptr;
}

RaftCommitTracker* RaftGroup::commitTracker() {
  if(ctptr == nullptr) {
    ctptr = new RaftCommitTracker(*journal());
  }
  return ctptr;
}

RaftWriteTracker* RaftGroup::writeTracker() {
  if(wtptr == nullptr) {
    wtptr = new RaftWriteTracker(*journal(), *state(), *stateMachine());
  }
  return wtptr;
}

RaftTrimmer* RaftGroup::trimmer() {
  if(trimmerptr == nullptr) {
    trimmerptr = new RaftTrimmer(*journal(), *config(), *stateMachine());
  }
  return trimmerptr;
}

RaftConfig* RaftGroup::config() {
  if(configptr == nullptr) {
    configptr = new RaftConfig(*dispatcher(), *stateMachine());
  }
  return configptr;
}
