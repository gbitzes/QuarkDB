// ----------------------------------------------------------------------
// File: Configuration.hh
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

#ifndef __QUARKDB_CONFIGURATION_H__
#define __QUARKDB_CONFIGURATION_H__

#include <string>
#include <vector>

#include <XrdOuc/XrdOucStream.hh>

#include "Common.hh"

namespace quarkdb {

enum class Mode {
  rocksdb = 0,
  raft = 1
};

class Configuration {
public:
  static bool fromFile(const std::string &filename, Configuration &out);
  static bool fromStream(XrdOucStream &stream, Configuration &out);
  static bool fromString(const std::string &str, Configuration &out);
  bool isValid();

  Mode getMode() { return mode; }
  std::string getDB() { return db; }
  TraceLevel getTraceLevel() { return trace; }

  std::vector<RaftServer> getNodes() { return nodes; }
  RaftServer getMyself() { return myself; }
  std::string getClusterID() { return clusterID; }
private:
  Mode mode;
  std::string db;
  TraceLevel trace = TraceLevel::info;

  // raft options
  std::vector<RaftServer> nodes;
  RaftServer myself;
  RaftClusterID clusterID;
};
}

#endif
