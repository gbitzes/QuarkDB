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

  redisDispatcher.reset(new RedisDispatcher(*stateMachine));
}

StateMachine* StandaloneGroup::getStateMachine() {
  return stateMachine;
}

Dispatcher* StandaloneGroup::getDispatcher() {
  return redisDispatcher.get();
}
