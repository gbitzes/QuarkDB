// ----------------------------------------------------------------------
// File: StandaloneGroup.cc
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

#include "StandaloneGroup.hh"
#include "ShardDirectory.hh"
#include "StateMachine.hh"
#include "redis/LeaseFilter.hh"
#include "Dispatcher.hh"
using namespace quarkdb;

StandaloneGroup::StandaloneGroup(ShardDirectory& dir, bool bulk)
: shardDirectory(dir), bulkload(bulk) {

  if(bulkload) {
      stateMachine = shardDirectory.getStateMachineForBulkload();
  }
  else {
      stateMachine = shardDirectory.getStateMachine();
  }

  publisher.reset(new Publisher());
  dispatcher.reset(new StandaloneDispatcher(*stateMachine, *publisher));
}

StandaloneGroup::~StandaloneGroup() {
}

StateMachine* StandaloneGroup::getStateMachine() {
  return stateMachine;
}

Dispatcher* StandaloneGroup::getDispatcher() {
  return dispatcher.get();
}

StandaloneDispatcher::StandaloneDispatcher(StateMachine &sm, Publisher &pub)
: stateMachine(&sm), dispatcher(sm), publisher(&pub) {}

LinkStatus StandaloneDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  // Show a user-friendly error message for raft-info, instead of
  // "internal dispatching error"
  if(req.getCommandType() == CommandType::RAFT) {
    qdb_warn("Received command " << req[0] << ", even though raft is not active");
    return conn->err(SSTR("raft not enabled, " << req[0] << " is unavailable, try quarkdb-info for general information"));
  }

  // Handle pubsub commands
  if(req.getCommandType() == CommandType::PUBSUB) {
    return publisher->dispatch(conn, req);
  }

  return dispatcher.dispatch(conn, req);
}

LinkStatus StandaloneDispatcher::dispatch(Connection *conn, Transaction &tx) {
  // Do lease filtering.
  ClockValue txTimestamp = stateMachine->getDynamicClock();
  LeaseFilter::transform(tx, txTimestamp);

  return dispatcher.dispatch(conn, tx);
}
