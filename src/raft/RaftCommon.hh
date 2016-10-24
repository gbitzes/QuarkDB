// ----------------------------------------------------------------------
// File: RaftCommon.hh
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

#ifndef __QUARKDB_RAFT_COMMON_H__
#define __QUARKDB_RAFT_COMMON_H__

#include "../Common.hh"
#include "../Utils.hh"

namespace quarkdb {

enum class RaftStatus {
  LEADER,
  FOLLOWER,
  CANDIDATE,
  OBSERVER,
  SHUTDOWN
};
std::string statusToString(RaftStatus st);

struct RaftEntry {
  RaftTerm term;
  RedisRequest request;
};

struct RaftAppendEntriesRequest {
  RaftTerm term;
  RaftServer leader;
  LogIndex prevIndex;
  RaftTerm prevTerm;
  LogIndex commitIndex;

  std::vector<RaftEntry> entries;
};

struct RaftAppendEntriesResponse {
  RaftAppendEntriesResponse(RaftTerm tr, LogIndex ind, bool out, const std::string &er)
  : term(tr), logSize(ind), outcome(out), err(er) {}

  RaftAppendEntriesResponse() {}

  RaftTerm term = -1;
  LogIndex logSize = -1;
  bool outcome = false;
  std::string err;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.push_back(std::to_string(term));
    ret.push_back(std::to_string(logSize));
    ret.push_back(std::to_string(outcome));
    ret.push_back(err);
    return ret;
  }
};

struct RaftVoteRequest {
  RaftTerm term;
  RaftServer candidate;
  LogIndex lastIndex;
  RaftTerm lastTerm;
};

struct RaftVoteResponse {
  RaftTerm term;
  bool granted;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.push_back(std::to_string(term));
    ret.push_back(std::to_string(granted));
    return ret;
  }

};

struct RaftInfo {
  RaftClusterID clusterID;
  RaftServer myself;
  RaftTerm term;
  LogIndex logSize;
  RaftStatus status;
  size_t blockedWrites;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.push_back(SSTR("TERM " << term));
    ret.push_back(SSTR("LOG-SIZE " << logSize));
    ret.push_back(SSTR("MYSELF " << myself.toString()));
    ret.push_back(SSTR("CLUSTER-ID " << clusterID));
    ret.push_back(SSTR("STATUS " << statusToString(status)));
    ret.push_back(SSTR("BLOCKED-WRITES " << blockedWrites));
    return ret;
  }
};

}

#endif
