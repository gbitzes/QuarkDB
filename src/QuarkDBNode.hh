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

#include <chrono>

#include "Dispatcher.hh"
#include "Configuration.hh"
#include "raft/RaftTimeouts.hh"

namespace quarkdb {

struct QuarkDBInfo {
  Mode mode;
  std::string baseDir;
  std::string version;
  std::string rocksdbVersion;
  size_t monitors;
  int64_t bootTime;
  int64_t uptime;

  std::vector<std::string> toVector() const;
};

class Shard; class ShardDirectory; class MultiOp;

class QuarkDBNode : public Dispatcher {
public:
  QuarkDBNode(const Configuration &config, const RaftTimeouts &t, ShardDirectory *injectedDirectory = nullptr);
  ~QuarkDBNode();

  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;

  const Configuration& getConfiguration() {
    return configuration;
  }

  Shard* getShard() {
    return shard.get();
  }

private:
  std::unique_ptr<ShardDirectory> shardDirectoryOwnership;
  std::unique_ptr<Shard> shard;

  Configuration configuration;
  ShardDirectory *shardDirectory;

  QuarkDBInfo info();

  std::atomic<bool> shutdown {false};
  const RaftTimeouts timeouts;

  std::chrono::steady_clock::time_point bootStart;
  std::chrono::steady_clock::time_point bootEnd;
};

}
#endif
