// ----------------------------------------------------------------------
// File: RaftMembers.hh
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

#ifndef QUARKDB_RAFT_MEMBERS_HH
#define QUARKDB_RAFT_MEMBERS_HH

#include "raft/RaftCommon.hh"
#include "Utils.hh"
#include "Common.hh"

namespace quarkdb {

// Public class to give out a consistent membership snapshot
struct RaftMembership {
  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;
  LogIndex epoch;

  bool contains(const RaftServer &target) {
    for(size_t i = 0; i < nodes.size(); i++) if(nodes[i] == target) return true;
    for(size_t i = 0; i < observers.size(); i++) if(observers[i] == target) return true;
    return false;
  }

  bool operator==(const RaftMembership &rhs) const {
    return nodes == rhs.nodes && observers == rhs.observers && epoch == rhs.epoch;
  }

  // Check if this node is "in limbo", that is the
  // initial, uninitialized state where we don't know
  // the members of this cluster
  bool inLimbo() const {
    return nodes.size() == 1 && nodes[0] == RaftServer::Null() && observers.empty();
  }

};

// Internal class, not exposed to users
struct RaftMembers {
  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;

  static RaftMembers LimboMembers() {
    RaftMembers limbo;
    limbo.nodes.emplace_back(RaftServer::Null());
    return limbo;
  }

  RaftMembers() {}

  RaftMembers(const std::vector<RaftServer> &_nodes, const std::vector<RaftServer> &_obs)
  : nodes(_nodes), observers(_obs) {}

  bool parse(std::string_view serialized) {
    nodes.clear();
    observers.clear();

    std::vector<std::string> parts = split(std::string(serialized), "|");
    if(parts.size() != 2) return false;

    if(!parseServers(parts[0], nodes)) {
      return false;
    }

    if(!parts[1].empty() && !parseServers(parts[1], observers)) {
      return false;
    }

    return true;
  }

  RaftMembers(std::string_view serialized) {
    if(!parse(serialized)) {
      qdb_throw("corruption, cannot parse members: " << serialized);
    }
  }

  bool addObserver(const RaftServer &observer, std::string &err) {
    if(contains(observers, observer)) {
      err = SSTR(observer.toString() << " is already an observer.");
      return false;
    }

    if(contains(nodes, observer)) {
      err = SSTR(observer.toString() << " is already a full node.");
      return false;
    }

    observers.push_back(observer);
    return true;
  }

  bool removeMember(const RaftServer &machine, std::string &err) {
    if(erase_element(observers, machine)) return true;
    if(erase_element(nodes, machine)) return true;

    err = SSTR(machine.toString() << " is neither an observer nor a full node.");
    return false;
  }

  bool promoteObserver(const RaftServer &observer, std::string &err) {
    if(erase_element(observers, observer)) {
      nodes.push_back(observer);
      return true;
    }

    err = SSTR(observer.toString() << " is not an observer.");
    return false;
  }

  std::string toString() const {
    std::ostringstream ss;
    ss << serializeNodes(nodes);
    ss << "|";
    ss << serializeNodes(observers);
    return ss.str();
  }

  bool operator==(const RaftMembers &rhs) const {
    return nodes == rhs.nodes && observers == rhs.observers;
  }
};
}
#endif
