// ----------------------------------------------------------------------
// File: RaftLease.cc
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
#include "RaftLease.hh"
#include "RaftUtils.hh"
using namespace quarkdb;

void RaftLastContact::heartbeat(const std::chrono::steady_clock::time_point &timepoint) {
  std::scoped_lock lock(mtx);

  if(lastCommunication < timepoint) {
    lastCommunication = timepoint;
  }
}

std::chrono::steady_clock::time_point RaftLastContact::get() {
  std::scoped_lock lock(mtx);
  return lastCommunication;
}

void RaftLease::updateTargets(const std::vector<RaftServer> &trgt) {
  std::scoped_lock lock(mtx);

  // clear the map of the old targets
  targets.clear();

  // update to new targets - the last contact details are NOT lost
  // for servers which exist in both sets!
  quorumSize = calculateQuorumSize(trgt.size() + 1);
  for(const RaftServer& target : trgt) {
    targets[target] = &this->getHandlerInternal(target);
  }
}

RaftLease::RaftLease(const std::vector<RaftServer> &trgt, const std::chrono::steady_clock::duration &leaseDur)
: leaseDuration(leaseDur) {
  updateTargets(trgt);
}

RaftLease::~RaftLease() {
  for(auto it = registrations.begin(); it != registrations.end(); it++) {
    delete it->second;
  }
}

//------------------------------------------------------------------------------
// The provided server may or may not be an actual target which influences
// the quorums.
//
// Register the endpoint if it hasn't been yet. RaftLease maintains ownership
// of the returned pointer.
//------------------------------------------------------------------------------
RaftLastContact& RaftLease::getHandlerInternal(const RaftServer &srv) {
  auto it = registrations.find(srv);

  if(it == registrations.end()) {
    registrations[srv] = new RaftLastContact(srv);
  }

  return *registrations[srv];
}

RaftLastContact& RaftLease::getHandler(const RaftServer &srv) {
  std::scoped_lock lock(mtx);
  return getHandlerInternal(srv);
}

//------------------------------------------------------------------------------
// Retrieve nth lease, starting from the end as sorted by most recent
// heartbeat. Assumes mtx is locked when calling this function!
//------------------------------------------------------------------------------
std::chrono::steady_clock::time_point RaftLease::getNthLease(size_t n) {
  if(quorumSize == 1) {
    // Special case: There's only a single node in our raft "cluster" - us.
    return std::chrono::steady_clock::now() + leaseDuration;
  }

  std::vector<std::chrono::steady_clock::time_point> leases;

  for(auto it = targets.begin(); it != targets.end(); it++) {
    leases.push_back(it->second->get());
  }

  std::sort(leases.begin(), leases.end());
  int threshold = (leases.size()+1) - n;

  if(threshold < 0) {
    threshold = 0;
  }
  return leases[threshold] + leaseDuration;
}

//------------------------------------------------------------------------------
// Only consider the targets when determining the deadline, and not any other
// registered endpoints. (they might be observers, which don't affect leases)
//------------------------------------------------------------------------------
std::chrono::steady_clock::time_point RaftLease::getDeadline() {
  std::scoped_lock lock(mtx);
  return getNthLease(quorumSize);
}

//------------------------------------------------------------------------------
// Return the timepoint at which the quorum would become shaky, meaning the
// loss of a single more node would cause the lease to expire and the cluster
// to go offline.
//------------------------------------------------------------------------------
steady_clock::time_point RaftLease::getShakyQuorumDeadline() {
  std::scoped_lock lock(mtx);
  return getNthLease(quorumSize + 1);
}
