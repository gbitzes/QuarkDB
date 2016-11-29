// ----------------------------------------------------------------------
// File: QuarkDBNode.cc
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

#include "QuarkDBNode.hh"
#include "Version.hh"

using namespace quarkdb;

QuarkDBNode::~QuarkDBNode() {
  detach();
  qdb_info("Shutting down QuarkDB node.")
}

//------------------------------------------------------------------------------
// Make this node backend-less.
// It will no longer be able to service user requests.
//------------------------------------------------------------------------------

void QuarkDBNode::detach() {
  if(!attached) return;
  attached = false;

  qdb_event("Received request to detach this node. Spinning until all requests being dispatched (" << beingDispatched << ") have been processed..");
  while(beingDispatched != 0) ;
  qdb_info("Requests being dispatched: " << beingDispatched << ", it is now safe to detach.");

  if(state) {
    qdb_info("Shutting down the raft machinery.");

    delete director;
    director = nullptr;

    delete dispatcher;
    dispatcher = nullptr;

    delete state;
    state = nullptr;

    delete raftClock;
    raftClock = nullptr;

    delete journal;
    journal = nullptr;
  }

  if(rocksdb) {
    qdb_info("Shutting down the main rocksdb store.");
    delete rocksdb;
    rocksdb = nullptr;
  }

  qdb_info("Backend has been detached from this quarkdb node.")
}

void QuarkDBNode::attach() {
  if(attached) return;

  rocksdb = new RocksDB(configuration.getDB());

  if(configuration.getMode() == Mode::rocksdb) {
    dispatcher = new RedisDispatcher(*rocksdb);
  }
  else if(configuration.getMode() == Mode::raft) {
    journal = new RaftJournal(configuration.getRaftLog());
    if(journal->getClusterID() != configuration.getClusterID()) {
      delete journal;
      qdb_throw("clusterID from configuration does not match the one stored in the journal");
    }

    state = new RaftState(*journal, configuration.getMyself());
    raftClock = new RaftClock(defaultTimeouts);
    RaftDispatcher *raftdispatcher = new RaftDispatcher(*journal, *rocksdb, *state, *raftClock);
    dispatcher = raftdispatcher;
    director = new RaftDirector(*raftdispatcher, *journal, *state, *raftClock);
  }
  else {
    qdb_throw("cannot determine configuration mode"); // should never happen
  }

  attached = true;
}

QuarkDBNode::QuarkDBNode(const Configuration &config, XrdBuffManager *buffManager, const std::atomic<int64_t> &inFlight_)
: configuration(config), bufferManager(buffManager), inFlight(inFlight_) {
  attach();
}

LinkStatus QuarkDBNode::dispatch(Connection *conn, RedisRequest &req) {
  auto it = redis_cmd_map.find(req[0]);
  if(it == redis_cmd_map.end()) return conn->err(SSTR("unknown command " << quotes(req[0])));

  RedisCommand cmd = it->second.first;
  switch(cmd) {
    case RedisCommand::QUARKDB_INFO: {
      return conn->vector(this->info());
    }
    case RedisCommand::QUARKDB_DETACH: {
      detach();
      return conn->ok();
    }
    case RedisCommand::QUARKDB_ATTACH: {
      attach();
      return conn->ok();
    }
    default: {
      if(!attached) return conn->err("node is detached from any backends");
      ScopedAdder<int64_t> adder(beingDispatched);
      if(!attached) return conn->err("node is detached from any backends");

      return dispatcher->dispatch(conn, req, 0);
    }
  }
}

std::vector<std::string> QuarkDBNode::info() {
  std::vector<std::string> ret;
  ret.emplace_back(SSTR("QUARKDB-VERSION " << STRINGIFY(VERSION_FULL)));
  ret.emplace_back(SSTR("ROCKSDB-VERSION " << ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH));
  ret.emplace_back(SSTR("IN-FLIGHT " << inFlight));

  return ret;
}
