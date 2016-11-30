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
#include <sys/stat.h>

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

static bool directoryExists(const std::string &path, std::string &err) {
  struct stat sb;

  if(stat(path.c_str(), &sb) != 0) {
    err = SSTR("Cannot stat " << path);
    return false;
  }

  if(!S_ISDIR(sb.st_mode)) {
    err = SSTR(path << " is not a directory");
    return false;
  }

  return true;
}

bool QuarkDBNode::attach(std::string &err) {
  if(attached) {
    err = "Node already attached.";
    return false;
  }

  std::string suberr;
  if(!directoryExists(configuration.getDatabase(), suberr)) {
    err = SSTR("Unable to attach to backend, cannot access base directory: " << suberr);
    return false;
  }

  if(configuration.getMode() == Mode::raft && !directoryExists(configuration.getRaftJournal(), suberr)) {
    err = SSTR("Unable to attach to backend (raft mode), cannot access raft journal: " << suberr);
    return false;
  }

  rocksdb = new RocksDB(configuration.getStateMachine());

  if(configuration.getMode() == Mode::rocksdb) {
    dispatcher = new RedisDispatcher(*rocksdb);
  }
  else if(configuration.getMode() == Mode::raft) {
    journal = new RaftJournal(configuration.getRaftJournal());
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
  return true;
}

QuarkDBNode::QuarkDBNode(const Configuration &config, XrdBuffManager *buffManager, const std::atomic<int64_t> &inFlight_)
: configuration(config), bufferManager(buffManager), inFlight(inFlight_) {
  std::string err;
  if(!attach(err)) qdb_critical(err);
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
      std::string err;
      if(attach(err)) return conn->ok();
      return conn->err(err);
    }
    default: {
      if(!attached) return conn->err("node is detached from any backends");
      ScopedAdder<int64_t> adder(beingDispatched);
      if(!attached) return conn->err("node is detached from any backends");

      return dispatcher->dispatch(conn, req, 0);
    }
  }
}

static std::string boolToString(bool b) {
  if(b) return "TRUE";
  return "FALSE";
}

static std::string modeToString(const Mode &mode) {
  if(mode == Mode::rocksdb) {
    return "STANDALONE";
  }
  if(mode == Mode::raft) {
    return "RAFT";
  }
  qdb_throw("unknown mode"); // should never happen
}

std::vector<std::string> QuarkDBNode::info() {
  std::vector<std::string> ret;
  ret.emplace_back(SSTR("ATTACHED " << boolToString(attached)));
  ret.emplace_back(SSTR("MODE " << modeToString(configuration.getMode())));
  ret.emplace_back(SSTR("BASE-DIRECTORY " << configuration.getDatabase()));
  ret.emplace_back(SSTR("QUARKDB-VERSION " << STRINGIFY(VERSION_FULL)));
  ret.emplace_back(SSTR("ROCKSDB-VERSION " << ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH));
  ret.emplace_back(SSTR("IN-FLIGHT " << inFlight));

  return ret;
}
