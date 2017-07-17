// ----------------------------------------------------------------------
// File: QuarkDBNode.hh
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

#ifndef __QUARKDB_NODE_H__
#define __QUARKDB_NODE_H__

#include "Dispatcher.hh"
#include "Configuration.hh"
#include "RedisParser.hh"
#include "StateMachine.hh"
#include "Dispatcher.hh"
#include "ShardDirectory.hh"
#include "raft/RaftJournal.hh"
#include "raft/RaftState.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftDirector.hh"
#include "raft/RaftGroup.hh"

namespace quarkdb {

struct QuarkDBInfo {
  Mode mode;
  std::string baseDir;
  std::string version;
  std::string rocksdbVersion;
  int64_t inFlight;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.emplace_back(SSTR("MODE " << modeToString(mode)));
    ret.emplace_back(SSTR("BASE-DIRECTORY " << baseDir));
    ret.emplace_back(SSTR("QUARKDB-VERSION " << version));
    ret.emplace_back(SSTR("ROCKSDB-VERSION " << rocksdbVersion));
    ret.emplace_back(SSTR("IN-FLIGHT " << inFlight));
    return ret;
  }
};

class Shard;

class QuarkDBNode : public Dispatcher {
public:
  QuarkDBNode(const Configuration &config, const std::atomic<int64_t> &inFlight_, const RaftTimeouts &t);
  ~QuarkDBNode();

  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;

  const Configuration& getConfiguration() {
    return configuration;
  }
private:
  Configuration configuration;
  ShardDirectory *shardDirectory;
  Shard *shard;

  QuarkDBInfo info();

  std::atomic<bool> shutdown {false};
  const std::atomic<int64_t> &inFlight;
  const RaftTimeouts timeouts;
  std::atomic<int64_t> beingDispatched {0};
};

}
#endif
