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
  std::lock_guard<std::mutex> lock(mtx);

  if(timepoint < lastCommunication) qdb_throw("attempted to push back lastCommunication for " << srv.toString());
  lastCommunication = timepoint;
}

std::chrono::steady_clock::time_point RaftLastContact::get() {
  std::lock_guard<std::mutex> lock(mtx);
  return lastCommunication;
}

void RaftLease::updateTargets(const std::vector<RaftServer> &trgt) {
  std::lock_guard<std::mutex> lock(mtx);

  // clear the map of the old targets
  targets.clear();

  // update to new targets - the last contact details are NOT lost
  // for servers which exist in both sets!
  quorumSize = calculateQuorumSize(trgt.size() + 1);
  for(const RaftServer& target : trgt) {
    targets[target] = this->getHandlerInternal(target);
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
RaftLastContact* RaftLease::getHandlerInternal(const RaftServer &srv) {
  auto it = registrations.find(srv);

  if(it == registrations.end()) {
    registrations[srv] = new RaftLastContact(srv);
  }

  return registrations[srv];
}

RaftLastContact* RaftLease::getHandler(const RaftServer &srv) {
  std::lock_guard<std::mutex> lock(mtx);
  return getHandlerInternal(srv);
}

//------------------------------------------------------------------------------
// Only consider the targets when determining the deadline, and not any other
// registered endpoints. (they might be observers, which don't affect leases)
//------------------------------------------------------------------------------
std::chrono::steady_clock::time_point RaftLease::getDeadline() {
  std::lock_guard<std::mutex> lock(mtx);
  std::vector<std::chrono::steady_clock::time_point> leases;

  for(auto it = targets.begin(); it != targets.end(); it++) {
    leases.push_back(it->second->get());
  }

  std::sort(leases.begin(), leases.end());
  size_t threshold = (leases.size()+1) - quorumSize;
  return leases[threshold] + leaseDuration;
}
