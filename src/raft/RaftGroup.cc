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
using namespace quarkdb;

RaftGroup::RaftGroup(const std::string &database, const RaftServer &myself, const RaftTimeouts &t)
: me(myself), timeouts(t) {
  journalptr = new RaftJournal(pathJoin(database, "raft-journal"));
  smptr = new StateMachine(pathJoin(database, "state-machine"));
}

RaftGroup::RaftGroup(RaftJournal &journal, StateMachine &stateMachine, const RaftServer &myself, const RaftTimeouts &t)
: me(myself), timeouts(t) {
  injectedDatabases = true;
  journalptr = &journal;
  smptr = &stateMachine;
}

RaftGroup::~RaftGroup() {
  spindown();
  if(!injectedDatabases) {
    delete journalptr;
    delete smptr;
  }
}

void RaftGroup::spinup() {
  director(); // transitively initializes everything
}

void RaftGroup::spindown() {
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
  return smptr;
}

RaftJournal* RaftGroup::journal() {
  // always available
  return journalptr;
}

RaftDispatcher* RaftGroup::dispatcher() {
  if(dispatcherptr == nullptr) {
    dispatcherptr = new RaftDispatcher(*journal(), *stateMachine(), *state(), *raftclock());
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
    directorptr = new RaftDirector(*dispatcher(), *journal(), *stateMachine(), *state(), *lease(), *commitTracker(), *raftclock());
  }
  return directorptr;
}

RaftReplicator* RaftGroup::replicator() {
  if(replicatorptr == nullptr) {
    replicatorptr = new RaftReplicator(*journal(), *stateMachine(), *state(), *lease(), *commitTracker(), raftclock()->getTimeouts());
  }
  return replicatorptr;
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
