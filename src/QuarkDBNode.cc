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
#include "ShardDirectory.hh"
#include "utils/FileUtils.hh"
#include "utils/ScopedAdder.hh"
#include "utils/TimeFormatting.hh"

#include <sys/stat.h>

using namespace quarkdb;

QuarkDBNode::~QuarkDBNode() {
  qdb_info("Shutting down QuarkDB node.")
}

QuarkDBNode::QuarkDBNode(const Configuration &config, const RaftTimeouts &t,
  ShardDirectory *injectedDirectory)

: configuration(config), timeouts(t) {

  bootStart = std::chrono::steady_clock::now();

  if(injectedDirectory) {
    shardDirectory = injectedDirectory; // no ownership!!!
  }
  else {
    shardDirectoryOwnership.reset(new ShardDirectory(configuration.getDatabase(), configuration));
    shardDirectory = shardDirectoryOwnership.get();
  }

  if(configuration.getMode() == Mode::raft) {
    shard.reset(new Shard(shardDirectory, configuration.getMyself(), configuration.getMode(), timeouts));
    if(!injectedDirectory) {
      shard->spinup();
    }
  }
  else {
    shard.reset(new Shard(shardDirectory, {}, configuration.getMode(), timeouts));
  }

  bootEnd = std::chrono::steady_clock::now();
}

LinkStatus QuarkDBNode::dispatch(Connection *conn, WriteBatch &batch) {
  return shard->dispatch(conn, batch);
}

LinkStatus QuarkDBNode::dispatch(Connection *conn, MultiOp &multiOp) {
  qdb_throw("NYI");
}

LinkStatus QuarkDBNode::dispatch(Connection *conn, RedisRequest &req) {
  switch(req.getCommand()) {
    case RedisCommand::PING: {
      return conn->raw(handlePing(req));
    }
    case RedisCommand::DEBUG: {
      if(req.size() != 2) return conn->errArgs(req[0]);
      if(caseInsensitiveEquals(req[1], "segfault")) {
        qdb_event("Performing harakiri on client request: SEGV");
        *( (int*) 42 ) = 5;
      }

      if(caseInsensitiveEquals(req[1], "kill")) {
        qdb_event("Performing harakiri on client request: SIGKILL");
        system(SSTR("kill -9 " << getpid()).c_str());
        return conn->ok();
      }

      if(caseInsensitiveEquals(req[1], "terminate")) {
        qdb_event("Performing harakiri on client request: SIGTERM");
        system(SSTR("kill " << getpid()).c_str());
        return conn->ok();
      }

      return conn->err(SSTR("unknown argument '" << req[1] << "'"));
    }
    case RedisCommand::CLIENT_ID: {
      return conn->status(conn->getID());
    }
    case RedisCommand::QUARKDB_INFO: {
      return conn->statusVector(this->info().toVector());
    }
    default: {
      return shard->dispatch(conn, req);
    }
  }
}

QuarkDBInfo QuarkDBNode::info() {
  return {configuration.getMode(), configuration.getDatabase(), VERSION_FULL_STRING, SSTR(ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH), shard->monitors(), std::chrono::duration_cast<std::chrono::seconds>(bootEnd - bootStart).count(), std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - bootEnd).count() };
}

std::vector<std::string> QuarkDBInfo::toVector() const {
  std::vector<std::string> ret;
  ret.emplace_back(SSTR("MODE " << modeToString(mode)));
  ret.emplace_back(SSTR("BASE-DIRECTORY " << baseDir));
  ret.emplace_back(SSTR("QUARKDB-VERSION " << version));
  ret.emplace_back(SSTR("ROCKSDB-VERSION " << rocksdbVersion));
  ret.emplace_back(SSTR("MONITORS " << monitors));
  ret.emplace_back(SSTR("BOOT-TIME " << bootTime << " (" << formatTime(std::chrono::seconds(bootTime)) << ")"));
  ret.emplace_back(SSTR("UPTIME " << uptime << " (" << formatTime(std::chrono::seconds(uptime)) << ")"));
  return ret;
}
