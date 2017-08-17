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

inline size_t calculateQuorumSize(size_t members) {
  return (members / 2) + 1;
}

struct ReplicaStatus {
  RaftServer target;
  bool online;
  LogIndex nextIndex;

  bool upToDate(LogIndex leaderLogSize) {
    return online && (leaderLogSize - nextIndex < 30000);
  }
};

struct ReplicationStatus {
  std::vector<ReplicaStatus> replicas;

  size_t replicasOnline() {
    size_t ret = 0;

    for(size_t i = 0; i < replicas.size(); i++) {
      if(replicas[i].online) {
        ret++;
      }
    }

    return ret;
  }

  size_t replicasUpToDate(LogIndex leaderLogSize) {
    size_t ret = 0;

    for(size_t i = 0; i < replicas.size(); i++) {
      if(replicas[i].upToDate(leaderLogSize)) {
        ret++;
      }
    }

    return ret;
  }

  bool quorumUpToDate(LogIndex leaderLogSize) {
    if(replicas.size() == 1) return false;
    return calculateQuorumSize(replicas.size()) <= replicasUpToDate(leaderLogSize);
  }

  ReplicaStatus getReplicaStatus(const RaftServer &replica) {
    for(size_t i = 0; i < replicas.size(); i++) {
      if(replicas[i].target == replica) {
        return replicas[i];
      }
    }

    qdb_throw("Replica " << " replica.target.toString() " << " not found");
  }

  void removeReplica(const RaftServer &replica) {
    for(size_t i = 0; i < replicas.size(); i++) {
      if(replicas[i].target == replica) {
        replicas.erase(replicas.begin()+i);
        return;
      }
    }

    qdb_throw("Replica " << " replica.target.toString() " << " not found");
  }

  void removeReplicas(const std::vector<RaftServer> &replicas) {
    for(size_t i = 0; i < replicas.size(); i++) {
      removeReplica(replicas[i]);
    }
  }

  void addReplica(const ReplicaStatus &replica) {
    for(size_t i = 0; i < replicas.size(); i++) {
      if(replicas[i].target == replica.target) {
        qdb_throw("Targer " << replica.target.toString() << " already exists in the list");
      }
    }
    replicas.push_back(replica);
  }

  bool contains(const RaftServer &replica) {
    for(size_t i = 0; i < replicas.size(); i++) {
      if(replicas[i].target == replica) return true;
    }
    return false;
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
  ReplicationStatus replicationStatus;

  std::vector<std::string> toVector() {
    std::vector<std::string> ret;
    ret.push_back(SSTR("TERM " << term));
    ret.push_back(SSTR("LOG-START " << logStart));
    ret.push_back(SSTR("LOG-SIZE " << logSize));
    ret.push_back(SSTR("LEADER " << leader.toString()));
    ret.push_back(SSTR("CLUSTER-ID " << clusterID));
    ret.push_back(SSTR("COMMIT-INDEX " << commitIndex));
    ret.push_back(SSTR("LAST-APPLIED " << lastApplied));
    ret.push_back(SSTR("BLOCKED-WRITES " << blockedWrites));

    ret.push_back("----------");
    ret.push_back(SSTR("MYSELF " << myself.toString()));
    ret.push_back(SSTR("STATUS " << statusToString(status)));

    ret.push_back("----------");
    ret.push_back(SSTR("MEMBERSHIP-EPOCH " << membershipEpoch));
    ret.push_back(SSTR("NODES " << serializeNodes(nodes)));
    ret.push_back(SSTR("OBSERVERS " << serializeNodes(observers)));

    if(!replicationStatus.replicas.empty()) {
      ret.push_back("----------");
    }

    for(auto it = replicationStatus.replicas.begin(); it != replicationStatus.replicas.end(); it++) {
      std::stringstream ss;
      ss << "REPLICA " << it->target.toString() << " ";
      if(it->online) {
        ss << "ONLINE | ";
        if(it->upToDate(logSize)) {
          ss << "UP-TO-DATE | ";
        }
        else {
          ss << "LAGGING    | ";
        }

        ss << "NEXT-INDEX " << it->nextIndex;
      }
      else {
        ss << "OFFLINE";
      }

      ret.push_back(ss.str());
    }

    return ret;
  }
};

}

#endif
