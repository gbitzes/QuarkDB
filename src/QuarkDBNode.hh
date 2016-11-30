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
#include "RocksDB.hh"
#include "Dispatcher.hh"
#include "raft/RaftJournal.hh"
#include "raft/RaftState.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftDirector.hh"

namespace quarkdb {

class QuarkDBNode {
public:
  QuarkDBNode(const Configuration &config, XrdBuffManager *buffManager, const std::atomic<int64_t> &inFlight_);
  ~QuarkDBNode();

  void detach();
  bool attach(std::string &err);
  LinkStatus dispatch(Connection *conn, RedisRequest &req);
private:
  Configuration configuration;

  Dispatcher* dispatcher = nullptr;
  RocksDB *rocksdb = nullptr;
  RaftJournal *journal = nullptr;
  RaftState *state = nullptr;
  RaftClock *raftClock = nullptr;
  RaftDirector *director = nullptr;

  XrdBuffManager *bufferManager = nullptr; // owned by xrootd, not me

  std::vector<std::string> info();

  std::atomic<bool> attached {false};
  const std::atomic<int64_t> &inFlight;
  std::atomic<int64_t> beingDispatched {0};
};

}
#endif
