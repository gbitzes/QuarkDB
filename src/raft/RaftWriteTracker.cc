// ----------------------------------------------------------------------
// File: RaftWriteTracker.cc
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

#include "RaftBlockedWrites.hh"
#include "RaftWriteTracker.hh"
#include "RaftJournal.hh"
#include "RaftState.hh"
#include "../StateMachine.hh"
#include "../Utils.hh"
using namespace quarkdb;

RaftWriteTracker::RaftWriteTracker(RaftJournal &jr, StateMachine &sm)
: journal(jr), stateMachine(sm), redisDispatcher(sm) {
  commitApplier = std::thread(&RaftWriteTracker::applyCommits, this);
}

RaftWriteTracker::~RaftWriteTracker() {
  shutdown = true;
  while(commitApplierActive) {
    journal.notifyWaitingThreads();
  }
  commitApplier.join();
  flushQueues("unavailable");
}

void RaftWriteTracker::applySingleCommit(LogIndex index) {
  // Determine if this particular index entry is associated to a request queue.
  std::shared_ptr<PendingQueue> blockedQueue = blockedWrites.popIndex(index);

  if(blockedQueue.get() == nullptr) {
    // this journal entry is not related to any connection,
    // let's just apply it manually from the journal
    RaftEntry entry;

    if(!journal.fetch(index, entry).ok()) {
      // serious error, threatens consistency. Bail out
      qdb_throw("failed to fetch log entry " << index << " when applying commits");
    }

    redisDispatcher.dispatch(entry.request, index);
    return;
  }

  LogIndex newBlockingIndex = blockedQueue->dispatchPending(&redisDispatcher, index);
  if(newBlockingIndex > 0) {
    if(newBlockingIndex <= index) qdb_throw("blocking index of queue went backwards: " << index << " => " << newBlockingIndex);
    blockedWrites.insert(newBlockingIndex, blockedQueue);
  }
}

void RaftWriteTracker::updatedCommitIndex(LogIndex commitIndex) {
  std::lock_guard<std::mutex> lock(mtx);
  for(LogIndex index = stateMachine.getLastApplied()+1; index <= commitIndex; index++) {
    applySingleCommit(index);
  }
}

void RaftWriteTracker::applyCommits() {
  LogIndex commitIndex = journal.getCommitIndex(); // local cached value
  while(journal.waitForCommits(commitIndex)) {
    if(shutdown) break;

    commitIndex = journal.getCommitIndex();
    updatedCommitIndex(journal.getCommitIndex());
  }
  commitApplierActive = false;
}

void RaftWriteTracker::flushQueues(const std::string &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  blockedWrites.flush(msg);
}

bool RaftWriteTracker::append(LogIndex index, RaftEntry &&entry, const std::shared_ptr<PendingQueue> &queue, RedisDispatcher &dispatcher) {
  std::lock_guard<std::mutex> lock(mtx);

  if(!journal.append(index, entry)) {
    qdb_critical("appending to journal failed for index = " << index <<
    " and term " << entry.term << " when appending to write tracker");
    return false;
  }

  blockedWrites.insert(index, queue);
  queue->addPendingRequest(&dispatcher, std::move(entry.request), index);
  return true;
}
