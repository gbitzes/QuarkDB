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

#include "StateMachine.hh"
#include "QuarkDBNode.hh"
#include "Version.hh"
#include "Shard.hh"
#include "utils/FileUtils.hh"
#include "utils/ScopedAdder.hh"

#include <sys/stat.h>

using namespace quarkdb;

QuarkDBNode::~QuarkDBNode() {
  delete shard;
  delete shardDirectory;

  qdb_info("Shutting down QuarkDB node.")
}

QuarkDBNode::QuarkDBNode(const Configuration &config, const RaftTimeouts &t)
: configuration(config), timeouts(t) {

  shardDirectory = new ShardDirectory(configuration.getDatabase(), configuration);

  if(configuration.getMode() == Mode::raft) {
    shard = new Shard(shardDirectory, configuration.getMyself(), configuration.getMode(), timeouts);
    // spin up the raft machinery
    shard->spinup();
  }
  else {
    shard = new Shard(shardDirectory, {}, configuration.getMode(), timeouts);
  }
}

LinkStatus QuarkDBNode::dispatch(Connection *conn, RedisRequest &req) {
  switch(req.getCommand()) {
    case RedisCommand::PING: {
      if(req.size() > 2) return conn->errArgs(req[0]);
      if(req.size() == 1) return conn->pong();

      return conn->string(req[1]);
    }
    case RedisCommand::DEBUG: {
      if(req.size() != 2) return conn->errArgs(req[0]);
      if(caseInsensitiveEquals(req[1], "segfault")) {
        qdb_critical("Performing harakiri on client request: SEGV");
        *( (int*) nullptr) = 5;
      }

      if(caseInsensitiveEquals(req[1], "kill")) {
        qdb_critical("Performing harakiri on client request: SIGKILL");
        system(SSTR("kill -9 " << getpid()).c_str());
        return conn->ok();
      }

      if(caseInsensitiveEquals(req[1], "terminate")) {
        qdb_critical("Performing harakiri on client request: SIGTERM");
        system(SSTR("kill " << getpid()).c_str());
        return conn->ok();
      }

      return conn->err(SSTR("unknown argument '" << req[1] << "'"));
    }
    case RedisCommand::QUARKDB_INFO: {
      return conn->vector(this->info().toVector());
    }
    default: {
      return shard->dispatch(conn, req);
    }
  }
}

QuarkDBInfo QuarkDBNode::info() {
  return {configuration.getMode(), configuration.getDatabase(), VERSION_FULL_STRING, SSTR(ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH) };
}
