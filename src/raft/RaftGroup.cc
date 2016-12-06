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
#include "../RocksDB.hh"
#include "RaftGroup.hh"
using namespace quarkdb;

RaftGroup::RaftGroup(const std::string &database, const RaftServer &myself, const RaftTimeouts &t)
: me(myself), timeouts(t) {
  journalptr = new RaftJournal(pathJoin(database, "raft-journal"));
  rocksdbptr = new RocksDB(pathJoin(database, "state-machine"));
}

RaftGroup::RaftGroup(RaftJournal &journal, RocksDB &stateMachine, const RaftServer &myself, const RaftTimeouts &t)
: me(myself), timeouts(t) {
  injectedDatabases = true;
  journalptr = &journal;
  rocksdbptr = &stateMachine;
}

RaftGroup::~RaftGroup() {
  spindown();
  if(!injectedDatabases) {
    delete journalptr;
    delete rocksdbptr;
  }
}

void RaftGroup::spinup() {
  director(); // transitively initializes everything
}

void RaftGroup::spindown() {
  // Delete everything except the journal and store
  if(directorptr) delete directorptr;
  if(dispatcherptr) delete dispatcherptr;
  if(replicatorptr) delete replicatorptr;
  if(stateptr) delete stateptr;
  if(clockptr) delete clockptr;
}

RaftServer RaftGroup::myself() {
  return me;
}

RocksDB* RaftGroup::rocksdb() {
  // always available
  return rocksdbptr;
}

RaftJournal* RaftGroup::journal() {
  // always available
  return journalptr;
}

RaftDispatcher* RaftGroup::dispatcher() {
  if(dispatcherptr == nullptr) {
    dispatcherptr = new RaftDispatcher(*journal(), *rocksdb(), *state(), *raftclock());
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
    directorptr = new RaftDirector(*dispatcher(), *journal(), *rocksdb(), *state(), *raftclock());
  }
  return directorptr;
}

RaftReplicator* RaftGroup::replicator() {
  if(replicatorptr == nullptr) {
    replicatorptr = new RaftReplicator(*journal(), *rocksdb(), *state(), raftclock()->getTimeouts());
  }
  return replicatorptr;
}
