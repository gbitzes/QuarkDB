// ----------------------------------------------------------------------
// File: Configuration.cc
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

#include <algorithm>

#include <unistd.h>
#include <fcntl.h>

#include "XrdRedisProtocol.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "Configuration.hh"
#include "Utils.hh"

using namespace quarkdb;

bool Configuration::fromFile(const std::string &filename, Configuration &out) {
  qdb_log("Reading configuration file from " << filename);

  XrdOucEnv myEnv;
  XrdOucStream stream(&XrdRedisProtocol::eDest, getenv("XRDINSTANCE"), &myEnv, "=====> ");

  int fd;

  if ((fd = open(filename.c_str(), O_RDONLY, 0)) < 0) {
    qdb_log("config: error " << errno << " when opening " << filename);
    return false;
  }

  stream.Attach(fd);
  return Configuration::fromStream(stream, out);
}

static bool fetchSingle(XrdOucStream &stream, std::string &dest) {
  char *val;
  if (!(val = stream.GetWord())) {
    return false;
  }

  dest = val;
  return true;
}

static bool parseMode(const std::string &buffer, Mode &mode) {
  if(buffer == "rocksdb") {
    mode = Mode::rocksdb;
  }
  else if(buffer == "raft") {
    mode = Mode::raft;
  }
  else {
    qdb_log("Unknown mode: " << quotes(buffer));
    return false;
  }

  return true;
}

static bool parseTraceLevel(const std::string &buffer, TraceLevel &trace) {
  if(buffer == "off") {
    trace = TraceLevel::off;
  }
  else if(buffer == "error") {
    trace = TraceLevel::error;
  }
  else if(buffer == "warn" || buffer == "warning") {
    trace = TraceLevel::warning;
  }
  else if(buffer == "info") {
    trace = TraceLevel::info;
  }
  else if(buffer == "debug" || buffer == "all") {
    trace = TraceLevel::debug;
  }
  else {
    qdb_log("Unknown trace level: " << quotes(buffer));
    return false;
  }

  return true;
}

bool Configuration::fromStream(XrdOucStream &stream, Configuration &out) {
  std::string buffer;
  char *option;

  while ((option = stream.GetMyFirstWord())) {
    bool ismine;
    if ((ismine = !strncmp("redis.", option, 6)) && option[6]) option += 6;

    if(ismine) {
      bool success;

      if(!strcmp("mode", option)) {
        success = fetchSingle(stream, buffer) && parseMode(buffer, out.mode);
      }
      else if(!strcmp("db", option)) {
        success = fetchSingle(stream, out.db);
      }
      else if(!strcmp("nodes", option)) {
        success = fetchSingle(stream, buffer) && parseServers(buffer, out.nodes);
      }
      else if(!strcmp("myself", option)) {
        success = fetchSingle(stream, buffer) && parseServer(buffer, out.myself);
      }
      else if(!strcmp("cluster_id", option)) {
        success = fetchSingle(stream, out.clusterID);
      }
      else if(!strcmp("trace", option)) {
        success = fetchSingle(stream, buffer) && parseTraceLevel(buffer, out.trace);
      }
      else {
        qdb_log("Error when parsing configuration - unknown option " << quotes(option));
        return false;
      }

      if(!success) {
        qdb_log("Error when parsing configuration option " << quotes("redis." << option));
        return false;
      }

    }
  }

  return out.isValid();
}

bool Configuration::fromString(const std::string &str, Configuration &out) {
  XrdOucStream stream(NULL, getenv("XRDINSTANCE"), NULL, "=====> ");

  int fd[2];
  if(pipe(fd) != 0) {
    throw FatalException("unable to create pipe");
  }

  if(write(fd[1], str.c_str(), str.size()) < 0) {
    throw FatalException("unable to write to pipe");
  }

  close(fd[1]);
  stream.Attach(fd[0]);
  return Configuration::fromStream(stream, out);
}

bool Configuration::isValid() {
  if(db.empty()) {
    qdb_log("redis.db must be specified.");
    return false;
  }

  bool raft = (mode == Mode::raft);

  if(raft == nodes.empty()) {
    qdb_log("redis.nodes is required when using raft and is incompatible with rocksdb");
    return false;
  }

  if(raft == myself.hostname.empty()) {
    qdb_log("redis.myself is required when using raft and is incompatible with rocksdb");
    return false;
  }

  if(raft == clusterID.empty()) {
    qdb_log("redis.cluster_id is required when using raft and is incompatible with rocksdb");
    return false;
  }

  // check that I'm part of the nodes
  if(raft) {
    if(std::find(nodes.begin(), nodes.end(), myself) == nodes.end()) {
      qdb_log("redis.nodes does not contain redis.myself");
      return false;
    }

    if(!checkUnique(nodes)) {
      qdb_log("redis.nodes contains duplicate entries.");
      return false;
    }
  }

  return true;
}
