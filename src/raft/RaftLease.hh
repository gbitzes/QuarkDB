// ----------------------------------------------------------------------
// File: RaftLease.hh
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

#ifndef __QUARKDB_RAFT_LEASE_H__
#define __QUARKDB_RAFT_LEASE_H__

#include <chrono>
#include <map>

#include "RaftCommon.hh"
#include "RaftMembers.hh"
#include "RaftTimeouts.hh"

namespace quarkdb {
using std::chrono::steady_clock;

class RaftLastContact {
public:
  RaftLastContact(const RaftServer &srv_) : srv(srv_) {}
  void heartbeat(const steady_clock::time_point &timepoint);
  steady_clock::time_point get();
private:
  steady_clock::time_point lastCommunication;
  std::mutex mtx;
  RaftServer srv;
};

class RaftLease {
public:
  RaftLease(const std::vector<RaftServer> &targets, const steady_clock::duration &leaseDuration);
  void updateTargets(const std::vector<RaftServer> &targets);
  ~RaftLease();
  RaftLastContact* getHandler(const RaftServer &srv);
  steady_clock::time_point getDeadline();
private:
  RaftLastContact* getHandlerInternal(const RaftServer &srv);

  std::mutex mtx;
  std::map<RaftServer, RaftLastContact*> targets;
  std::map<RaftServer, RaftLastContact*> registrations;
  steady_clock::duration leaseDuration;
  size_t quorumSize;
};

}

#endif
