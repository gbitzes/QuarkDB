// ----------------------------------------------------------------------
// File: RaftCommitTracker.cc
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

#include "RaftCommitTracker.hh"
#include <random>
#include <algorithm>
using namespace quarkdb;

RaftMatchIndexTracker::RaftMatchIndexTracker(RaftCommitTracker &tr, const RaftServer &srv) {
  reset(tr, srv);
}

void RaftMatchIndexTracker::reset() {
  if(tracker) {
    tracker->deregister(*this);
    tracker = nullptr;
    it = std::map<RaftServer, LogIndex>::iterator();
  }
}

void RaftMatchIndexTracker::reset(RaftCommitTracker &tr, const RaftServer &srv) {
  reset();
  tracker = &tr;
  it = tracker->registration(srv);
}

RaftMatchIndexTracker::RaftMatchIndexTracker() { }

RaftMatchIndexTracker::~RaftMatchIndexTracker() {
  reset();
}

void RaftMatchIndexTracker::update(LogIndex newMatchIndex) {
  if(tracker) {
    if(newMatchIndex < it->second) qdb_throw("attempted to reduce matchIndex: " << it->second << " ==> " << newMatchIndex);
    it->second = newMatchIndex;
    tracker->updated(newMatchIndex);
  }
}

RaftCommitTracker::RaftCommitTracker(RaftState &st, size_t quorumSize)
: state(st) {
  updateQuorum(quorumSize);
}

RaftCommitTracker::~RaftCommitTracker() { }

std::map<RaftServer, LogIndex>::iterator RaftCommitTracker::registration(const RaftServer &srv) {
  std::lock_guard<std::mutex> lock(mtx);

  if(matchIndex.count(srv) > 0) qdb_throw(srv.toString() << " is already being tracked");
  matchIndex[srv] = 0;
  return matchIndex.find(srv);
}

void RaftCommitTracker::deregister(RaftMatchIndexTracker &tracker) {
  std::lock_guard<std::mutex> lock(mtx);
  matchIndex.erase(tracker.it);
}

void RaftCommitTracker::updateQuorum(size_t newQuorum) {
  std::lock_guard<std::mutex> lock(mtx);
  if(newQuorum < 2) qdb_throw("quorum cannot be smaller than 2");
  qdb_info("Updating commit tracker quorum size: " << quorum << " ==> " << newQuorum);
  quorum = newQuorum;
}

void RaftCommitTracker::recalculateCommitIndex() {
  // remember, we also take into account the current node, which is a leader.
  // (otherwise we wouldn't be running the commit tracker)
  // The leader is by definition always up-to-date, so we don't run
  // a RaftMatchIndexTracker on it. But it has to be taken into account in the
  // commitIndex calculation.

  if(matchIndex.size() < quorum-1) return;

  std::vector<LogIndex> sortedVector;
  for(std::map<RaftServer, LogIndex>::iterator it = matchIndex.begin();
      it != matchIndex.end(); it++) {
    sortedVector.push_back(it->second);
  }

  std::sort(sortedVector.begin(), sortedVector.end());
  size_t threshold = sortedVector.size() - (quorum-1);

  LogIndex stateCommitIndex = state.getCommitIndex();
  if(sortedVector[threshold] < stateCommitIndex) {
    qdb_warn("calculated a commitIndex which is smaller than state.commitIndex: " << sortedVector[threshold] << ", " << stateCommitIndex << ". Will be unable to commit new entries until this is resolved.");
  }
  else {
    commitIndex = sortedVector[threshold];
    state.setCommitIndex(commitIndex);
  }
}

void RaftCommitTracker::updated(LogIndex val) {
  std::lock_guard<std::mutex> lock(mtx);
  if(val <= commitIndex) return; // nothing to do, we've already notified state of the change
  recalculateCommitIndex();
}
