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

#include "RaftCommon.hh"
#include "../Utils.hh"
#include "../Common.hh"

#ifndef __QUARKDB_RAFT_MEMBERS_H__
#define __QUARKDB_RAFT_MEMBERS_H__

namespace quarkdb {

// Public class to give out a consistent membership snapshot
struct RaftMembership {
  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;
  LogIndex epoch;
};

// Internal class, not exposed to users
struct RaftMembers {
  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;

  RaftMembers() {}

  RaftMembers(const std::vector<RaftServer> &_nodes, const std::vector<RaftServer> &_obs)
  : nodes(_nodes), observers(_obs) {}

  RaftMembers(const std::string &serialized) {
    std::vector<std::string> parts = split(serialized, "|");
    if(parts.size() != 2) qdb_throw("corruption, unable to parse raft members: " << serialized);

    if(!parseServers(parts[0], nodes)) {
      qdb_throw("corruption, cannot parse nodes: " << parts[0]);
    }

    if(!parts[1].empty() && !parseServers(parts[1], observers)) {
      qdb_throw("corruption, cannot parse observers: " << parts[1]);
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
