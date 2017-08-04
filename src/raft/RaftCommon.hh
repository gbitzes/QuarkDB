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

#include <string.h>
#include "../Common.hh"
#include "../Utils.hh"

namespace quarkdb {

enum class RaftStatus {
  LEADER,
  FOLLOWER,
  CANDIDATE,
  SHUTDOWN
};
std::string statusToString(RaftStatus st);

inline void append_int_to_string(int64_t source, std::ostringstream &target) {
  char buff[sizeof(source)];
  memcpy(&buff, &source, sizeof(source));
  target.write(buff, sizeof(source));
}

inline int64_t fetch_int_from_string(const char *pos) {
  int64_t result;
  memcpy(&result, pos, sizeof(result));
  return result;
}

struct RaftEntry {
  RaftTerm term;
  RedisRequest request;

  RaftEntry() {}

  RaftEntry(RaftTerm term_, RedisRequest&& req) : term(term_), request(std::move(req)) {}
  RaftEntry(RaftTerm term_, const RedisRequest& req) : term(term_), request(req) {}

  template<typename... Args>
  RaftEntry(RaftTerm term_, Args&&... args) : term(term_), request{args...} {}

  std::string serialize() const {
    std::ostringstream ss;
    append_int_to_string(term, ss);

    for(size_t i = 0; i < request.size(); i++) {
      append_int_to_string(request[i].size(), ss);
      ss << request[i];
    }

    return ss.str();
  }

  static void deserialize(RaftEntry &entry, const std::string &data) {
    entry.request.clear();
    entry.term = fetch_int_from_string(data.c_str());

    const char *pos = data.c_str() + sizeof(term);
    const char *end = data.c_str() + data.size();

    while(pos < end) {
      int64_t len = fetch_int_from_string(pos);
      pos += sizeof(len);

      entry.request.emplace_back(pos, len);
      pos += len;
    }
  }

  bool operator==(const RaftEntry &rhs) const {
    return term == rhs.term && request == rhs.request;
  }

  bool operator!=(const RaftEntry &rhs) const {
    return !(*this == rhs);
  }
};

inline std::ostream& operator<<(std::ostream& out, const RaftEntry& entry) {
  out << "term: " << entry.term << " -> " << entry.request;
  return out;
}

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

enum class RaftVote {
  VETO = -1,
  REFUSED = 0,
  GRANTED = 1
};

struct RaftVoteResponse {
  RaftTerm term;
  RaftVote vote;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.push_back(std::to_string(term));

    if(vote == RaftVote::GRANTED) {
      ret.push_back("granted");
    }
    else if(vote == RaftVote::REFUSED) {
      ret.push_back("refused");
    }
    else if(vote == RaftVote::VETO) {
      ret.push_back("veto");
    }
    else {
      qdb_throw("unable to convert vote to string in RaftVoteResponse::toVector");
    }

    return ret;
  }

};

struct RaftInfo {
  RaftClusterID clusterID;
  RaftServer myself;
  RaftServer leader;
  LogIndex membershipEpoch;
  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;
  RaftTerm term;
  LogIndex logStart;
  LogIndex logSize;
  RaftStatus status;
  LogIndex commitIndex;
  LogIndex lastApplied;
  size_t blockedWrites;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.push_back(SSTR("TERM " << term));
    ret.push_back(SSTR("LOG-START " << logStart));
    ret.push_back(SSTR("LOG-SIZE " << logSize));
    ret.push_back(SSTR("MYSELF " << myself.toString()));
    ret.push_back(SSTR("LEADER " << leader.toString()));
    ret.push_back(SSTR("MEMBERSHIP-EPOCH " << membershipEpoch));
    ret.push_back(SSTR("NODES " << serializeNodes(nodes)));
    ret.push_back(SSTR("OBSERVERS " << serializeNodes(observers)));
    ret.push_back(SSTR("CLUSTER-ID " << clusterID));
    ret.push_back(SSTR("STATUS " << statusToString(status)));
    ret.push_back(SSTR("COMMIT-INDEX " << commitIndex));
    ret.push_back(SSTR("LAST-APPLIED " << lastApplied));
    ret.push_back(SSTR("BLOCKED-WRITES " << blockedWrites));
    return ret;
  }
};

}

#endif
