// ----------------------------------------------------------------------
// File: Shard.hh
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

#ifndef __QUARKDB_SHARD_H__
#define __QUARKDB_SHARD_H__

#include "raft/RaftTimeouts.hh"
#include "Dispatcher.hh"
#include "Configuration.hh"

namespace quarkdb {

struct ShardInfo {
  bool resilvering;
  Mode mode;
  std::string shardDirectory;
  int64_t inFlight;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.emplace_back(SSTR("BEING-RESILVERED " << boolToString(resilvering)));
    ret.emplace_back(SSTR("MODE " << modeToString(mode)));
    ret.emplace_back(SSTR("SHARD-DIRECTORY" << shardDirectory));
    ret.emplace_back(SSTR("IN-FLIGHT " << inFlight));
    return ret;
  }
};

class RaftGroup; class ShardDirectory;
class Shard : public Dispatcher {
public:
  Shard(ShardDirectory *shardDir, const RaftServer &me, Mode mode, const RaftTimeouts &t);
  ~Shard();

  RaftGroup* getRaftGroup();
  void spinup();
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;
private:
  void detach();
  void attach();

  std::atomic<bool> attached {false};
  std::atomic<bool> shutdown;
  std::atomic<int64_t> beingDispatched {0};

  ShardDirectory *shardDirectory;

  RaftGroup *raftGroup = nullptr;
  StateMachine *stateMachine = nullptr;
  Dispatcher *dispatcher = nullptr;

  RaftServer myself;
  Mode mode;
  RaftTimeouts timeouts;
};

}

#endif
