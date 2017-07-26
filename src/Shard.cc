// ----------------------------------------------------------------------
// File: Shard.cc
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

#include "StateMachine.hh"
#include "Shard.hh"
#include "ShardDirectory.hh"
#include "raft/RaftGroup.hh"
#include "raft/RaftDispatcher.hh"
#include "utils/ScopedAdder.hh"

using namespace quarkdb;

Shard::Shard(ShardDirectory *shardDir, const RaftServer &me, Mode m, const RaftTimeouts &t)
: shardDirectory(shardDir), myself(me), mode(m), timeouts(t), inFlightTracker(false) {
  attach();
}

void Shard::attach() {
  qdb_assert(!inFlightTracker.isAcceptingRequests());

  if(mode == Mode::standalone) {
    stateMachine = shardDirectory->getStateMachine();
    dispatcher = new RedisDispatcher(*stateMachine);
  }
  else if(mode == Mode::raft) {
    raftGroup = new RaftGroup(*shardDirectory, myself, timeouts);
    dispatcher = static_cast<Dispatcher*>(raftGroup->dispatcher());
  }
  else {
    qdb_throw("cannot determine configuration mode"); // should never happen
  }

  inFlightTracker.setAcceptingRequests(true);
}

void Shard::start() {
  attach();
  spinup();
}

void Shard::detach() {
  if(!inFlightTracker.isAcceptingRequests()) return;
  inFlightTracker.setAcceptingRequests(false);

  qdb_event("Detaching this shard - spinning until all requests being dispatched (" << inFlightTracker.getInFlight() << ") have been processed.");
  inFlightTracker.spinUntilNoRequestsInFlight();
  qdb_info("All requests processed, detaching.");

  if(raftGroup) {
    qdb_info("Shutting down the raft machinery.");
    delete raftGroup;
    raftGroup = nullptr;
  }
  else if(stateMachine) {
    qdb_info("Shutting down the state machine.");
    delete stateMachine;
    stateMachine = nullptr;

    delete dispatcher;
    dispatcher = nullptr;
  }

  qdb_info("Backend has been detached from this quarkdb shard.");
}

Shard::~Shard() {
  detach();
}

RaftGroup* Shard::getRaftGroup() {
  return raftGroup;
}

void Shard::spinup() {
  raftGroup->spinup();
}

LinkStatus Shard::dispatch(Connection *conn, RedisRequest &req) {
  auto it = redis_cmd_map.find(req[0]);
  if(it == redis_cmd_map.end()) return conn->err(SSTR("unknown command " << quotes(req[0])));

  RedisCommand cmd = it->second.first;

  switch(cmd) {
    case RedisCommand::QUARKDB_START_RESILVERING: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      if(req.size() != 2) return conn->errArgs(req[0]);

      ResilveringEventID eventID = req[1];

      std::string err;
      if(!shardDirectory->resilveringStart(eventID, err)) {
        return conn->err(err);
      }

      return conn->ok();
    }
    case RedisCommand::QUARKDB_RESILVERING_COPY_FILE: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      if(req.size() != 4) return conn->errArgs(req[0]);

      ResilveringEventID eventID = req[1];

      std::string err;
      if(!shardDirectory->resilveringCopy(eventID, req[2], req[3], err)) {
        return conn->err(err);
      }

      return conn->ok();
    }
    case RedisCommand::QUARKDB_FINISH_RESILVERING: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      if(req.size() != 2) return conn->errArgs(req[0]);

      ResilveringEventID eventID = req[1];

      detach();

      std::string err;
      if(!shardDirectory->resilveringFinish(eventID, err)) {
        start();
        return conn->err(err);
      }

      start();
      return conn->ok();
    }
    default: {
      InFlightRegistration registration(inFlightTracker);
      if(!registration.ok()) {
        return conn->err("unavailable");
      }

      return dispatcher->dispatch(conn, req);
    }
  }
}
