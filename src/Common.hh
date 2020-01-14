// ----------------------------------------------------------------------
// File: Common.hh
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

#ifndef QUARKDB_COMMON_HH
#define QUARKDB_COMMON_HH

#include <vector>
#include <string>
#include "utils/Macros.hh"

namespace quarkdb {

enum class TraceLevel {
  off = 0,
  error = 1,
  warning = 2,
  info = 3,
  debug = 4
};

struct RaftServer {
  std::string hostname;
  int port = 0;

  static RaftServer Null() {
    RaftServer srv;

    srv.hostname = "#!^NULL-HOSTNAME^!#";
    srv.port = 0;
    return srv;
  }

  RaftServer() {}
  RaftServer(std::string_view h, int p) : hostname(h), port(p) {}

  bool operator==(const RaftServer& rhs) const {
    return hostname == rhs.hostname && port == rhs.port;
  }

  bool operator!=(const RaftServer& rhs) const {
    return !(*this == rhs);
  }

  bool operator<(const RaftServer &rhs) const {
    if(hostname != rhs.hostname) {
      return hostname < rhs.hostname;
    }
    return port < rhs.port;
  }

  std::string toString() const {
    if(hostname.empty()) return "";
    return hostname + ":" + std::to_string(port);
  }

  bool empty() const {
    return hostname.empty();
  }

  void clear() {
    hostname.clear();
    port = 0;
  }
};

enum class FsyncPolicy {
  kAlways,
  kSyncImportantUpdates,
  kAsync // async at an undefined future time
};

inline std::string fsyncPolicyToString(FsyncPolicy pol) {
  switch(pol) {
    case FsyncPolicy::kAlways: {
      return "always";
    }
    case FsyncPolicy::kSyncImportantUpdates: {
      return "sync-important-updates";
    }
    case FsyncPolicy::kAsync: {
      return "async";
    }
    default: {
      qdb_throw("Unknown FsyncPolicy, should never happen");
    }
  }
}

inline bool parseFsyncPolicy(std::string_view s, FsyncPolicy &out) {
  if(s == "always") {
    out = FsyncPolicy::kAlways;
    return true;
  }

  if(s == "sync-important-updates") {
    out = FsyncPolicy::kSyncImportantUpdates;
    return true;
  }

  if(s == "async") {
    out = FsyncPolicy::kAsync;
    return true;
  }

  return false;
}

using RaftClusterID = std::string;
using RaftTerm = int64_t;
using LogIndex = int64_t;
using ClockValue = uint64_t;

}

#endif
