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

#include "RaftUtils.hh"
#include "RaftCommitTracker.hh"
#include "RaftJournal.hh"
#include <random>
#include <algorithm>
using namespace quarkdb;

RaftMatchIndexTracker::RaftMatchIndexTracker(RaftCommitTracker &tr, const RaftServer &srv)
: tracker(tr), server(srv) {
}

void RaftMatchIndexTracker::update(LogIndex newMatchIndex) {
  if(newMatchIndex < matchIndex) qdb_throw("attempted to reduce matchIndex: " << matchIndex << " ==> " << newMatchIndex);
  matchIndex = newMatchIndex;
  tracker.updated(matchIndex);
}

RaftCommitTracker::RaftCommitTracker(RaftJournal &jr)
: journal(jr) {
  updateTargets(journal.getMembership().nodes);
}

RaftCommitTracker::~RaftCommitTracker() {
  reset();
}

void RaftCommitTracker::reset() {
  for(auto it = registrations.begin(); it != registrations.end(); it++) {
    delete it->second;
  }
  registrations.clear();
  commitIndex = 0;
}

RaftMatchIndexTracker& RaftCommitTracker::getHandler(const RaftServer &srv) {
  std::lock_guard<std::mutex> lock(mtx);
  return this->getHandlerInternal(srv);
}

RaftMatchIndexTracker& RaftCommitTracker::getHandlerInternal(const RaftServer &srv) {
  auto it = registrations.find(srv);

  if(it == registrations.end()) {
    registrations[srv] = new RaftMatchIndexTracker(*this, srv);
  }

  return *registrations[srv];
}

void RaftCommitTracker::updateTargets(const std::vector<RaftServer> &trgt) {
  std::lock_guard<std::mutex> lock(mtx);

  // clear the map of the old targets
  targets.clear();

  // update to new targets - the matchIndex is NOT lost
  // for servers which exist in both sets!
  quorumSize = calculateQuorumSize(trgt.size() + 1);
  if(quorumSize < 2) qdb_throw("quorum size cannot be smaller than 2");
  for(const RaftServer& target : trgt) {
    targets[target] = &this->getHandlerInternal(target);
  }
}

void RaftCommitTracker::updateCommitIndex(LogIndex newCommitIndex) {
  LogIndex journalCommitIndex = journal.getCommitIndex();
  if(newCommitIndex < journalCommitIndex) {
    qdb_warn("calculated a commitIndex which is smaller than journal.commitIndex: " << newCommitIndex << ", " << journalCommitIndex << ". Will be unable to commit new entries until this is resolved.");
    commitIndexLagging = true;
  }
  else {
    if(commitIndexLagging) {
      qdb_info("commitIndex no longer lagging behind journal.commitIndex, committing of new entries is now possible again.");
      commitIndexLagging = false;
    }
    commitIndex = newCommitIndex;
    journal.setCommitIndex(commitIndex);
  }
}

void RaftCommitTracker::recalculateCommitIndex() {
  // remember, we also take into account the current node, which is a leader.
  // (otherwise we wouldn't be running the commit tracker)
  // The leader is by definition always up-to-date, so we don't run
  // a RaftMatchIndexTracker on it. But it has to be taken into account in the
  // commitIndex calculation.

  std::vector<LogIndex> matchIndexes;

  for(auto it = targets.begin(); it != targets.end(); it++) {
    matchIndexes.push_back(it->second->get());
  }

  std::sort(matchIndexes.begin(), matchIndexes.end());
  size_t threshold = (matchIndexes.size()+1) - quorumSize;
  updateCommitIndex(matchIndexes[threshold]);
}

void RaftCommitTracker::updated(LogIndex val) {
  std::lock_guard<std::mutex> lock(mtx);
  if(val <= commitIndex) return; // nothing to do, we've already notified journal of the change
  recalculateCommitIndex();
}
