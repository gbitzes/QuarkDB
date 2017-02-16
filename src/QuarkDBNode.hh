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
#include "raft/RaftJournal.hh"
#include "raft/RaftState.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftDirector.hh"
#include "raft/RaftGroup.hh"

namespace quarkdb {

inline std::string modeToString(const Mode &mode) {
  if(mode == Mode::standalone) {
    return "STANDALONE";
  }
  if(mode == Mode::raft) {
    return "RAFT";
  }
  qdb_throw("unknown mode"); // should never happen
}

struct QuarkDBInfo {
  bool attached;
  bool resilvering;
  Mode mode;
  std::string baseDir;
  std::string version;
  std::string rocksdbVersion;
  int64_t inFlight;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.emplace_back(SSTR("ATTACHED " << boolToString(attached)));
    ret.emplace_back(SSTR("BEING-RESILVERED " << boolToString(resilvering)));
    ret.emplace_back(SSTR("MODE " << modeToString(mode)));
    ret.emplace_back(SSTR("BASE-DIRECTORY " << baseDir));
    ret.emplace_back(SSTR("QUARKDB-VERSION " << version));
    ret.emplace_back(SSTR("ROCKSDB-VERSION " << rocksdbVersion));
    ret.emplace_back(SSTR("IN-FLIGHT " << inFlight));
    return ret;
  }
};

class QuarkDBNode : public Dispatcher {
public:
  QuarkDBNode(const Configuration &config, const std::atomic<int64_t> &inFlight_, const RaftTimeouts &t = defaultTimeouts);
  ~QuarkDBNode();

  void detach();
  bool attach(std::string &err);
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;
private:
  Configuration configuration;
  RaftGroup* raftgroup = nullptr;
  StateMachine *stateMachine = nullptr;
  Dispatcher* dispatcher = nullptr;

  QuarkDBInfo info();

  std::atomic<bool> attached {false};
  const std::atomic<int64_t> &inFlight;
  const RaftTimeouts timeouts;
  std::atomic<int64_t> beingDispatched {0};

  void cancelResilvering();
  std::atomic<bool> resilvering {false};
};

}
#endif
