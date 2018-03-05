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
#include "redis/MultiOp.hh"
#include "utils/ScopedAdder.hh"
#include "utils/RequestCounter.hh"

using namespace quarkdb;

Shard::Shard(ShardDirectory *shardDir, const RaftServer &me, Mode m, const RaftTimeouts &t)
: shardDirectory(shardDir), myself(me), mode(m), timeouts(t), inFlightTracker(false),
  requestCounter(std::chrono::seconds(10)) {
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
  else if(mode == Mode::bulkload) {
    stateMachine = shardDirectory->getStateMachineForBulkload();
    dispatcher = new RedisDispatcher(*stateMachine);
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

void Shard::stopAcceptingRequests() {
  inFlightTracker.setAcceptingRequests(false);
  qdb_event("Spinning until all requests being dispatched (" << inFlightTracker.getInFlight() << ") have been processed.");
  inFlightTracker.spinUntilNoRequestsInFlight();
}

void Shard::detach() {
  if(!inFlightTracker.isAcceptingRequests()) return;
  stopAcceptingRequests();
  qdb_info("All requests processed, detaching.");

  if(raftGroup) {
    qdb_info("Shutting down the raft machinery.");
    delete raftGroup;
    raftGroup = nullptr;
  }
  else if(stateMachine) {
    // The state machine is owned by ShardDirectory, so, don't delete it
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
  std::lock_guard<std::mutex> lock(raftGroupMtx);
  return raftGroup;
}

void Shard::spinup() {
  raftGroup->spinup();
  dispatcher = static_cast<Dispatcher*>(raftGroup->dispatcher());
}

void Shard::spindown() {
  raftGroup->spindown();
}

LinkStatus Shard::dispatch(Connection *conn, WriteBatch &batch) {
  InFlightRegistration registration(inFlightTracker);
  if(!registration.ok()) {
    return conn->err("unavailable");
  }

  for(size_t i = 0; i < batch.requests.size(); i++) {
    commandMonitor.broadcast(conn->describe(), batch.requests[i]);
  }

  LinkStatus ret = dispatcher->dispatch(conn, batch);
  requestCounter.account(batch);
  return ret;
}

LinkStatus Shard::dispatch(Connection *conn, RedisRequest &req) {
  commandMonitor.broadcast(conn->describe(), req);

  switch(req.getCommand()) {
    case RedisCommand::MONITOR: {
      commandMonitor.addRegistration(conn);
      return conn->ok();
    }
    case RedisCommand::INVALID: {
      qdb_warn("Received unrecognized command: " << quotes(req[0]));
      return conn->err(SSTR("unknown command " << quotes(req[0])));
    }
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

      std::lock_guard<std::mutex> lock(raftGroupMtx);
      detach();

      std::string err;
      if(!shardDirectory->resilveringFinish(eventID, err)) {
        start();
        return conn->err(err);
      }

      start();
      return conn->ok();
    }
    case RedisCommand::QUARKDB_BULKLOAD_FINALIZE: {
      if(req.size() != 1) return conn->errArgs(req[0]);
      if(mode != Mode::bulkload) {
        qdb_warn("received command QUARKDB_BULKLOAD_FINALIZE while in mode " << modeToString(mode));
        return conn->err("not in bulkload mode");
      }

      stopAcceptingRequests();
      stateMachine->finalizeBulkload();
      return conn->ok();
    }
    default: {
      if(req.getCommandType() == CommandType::QUARKDB) {
        qdb_critical("Unable to dispatch command '" << req[0] << "' of type QUARKDB");
        return conn->err(SSTR("internal dispatching error"));
      }

      InFlightRegistration registration(inFlightTracker);
      if(!registration.ok()) {
        return conn->err("unavailable");
      }

      LinkStatus ret = dispatcher->dispatch(conn, req);
      requestCounter.account(req);
      return ret;
    }
  }
}
