// ----------------------------------------------------------------------
// File: RaftState.hh
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

#ifndef __QUARKDB_RAFT_STATE_H__
#define __QUARKDB_RAFT_STATE_H__

#include <mutex>
#include <condition_variable>
#include <memory>
#include "../Common.hh"
#include "RaftCommon.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class RaftJournal;

struct RaftStateSnapshot {
  RaftTerm term;
  RaftStatus status;
  RaftServer leader;
  RaftServer votedFor;
  LogIndex leadershipMarker;
  std::chrono::steady_clock::time_point timeCreated;

  RaftStateSnapshot() : term(-1), status(RaftStatus::FOLLOWER), leadershipMarker(-1) {
    timeCreated = std::chrono::steady_clock::now();
  }

  RaftStateSnapshot(RaftTerm trm, RaftStatus st, const RaftServer &ld,
    const RaftServer &vote, LogIndex marker) : term(trm), status(st), leader(ld),
    votedFor(vote), leadershipMarker(marker) {
    timeCreated = std::chrono::steady_clock::now();
  }

  bool equals(const RaftStateSnapshot &rhs) const {
    // We don't check timeCreated, this is on purpose.
    return term == rhs.term && status == rhs.status && leader == rhs.leader &&
           votedFor == rhs.votedFor && leadershipMarker == rhs.leadershipMarker;
  }

  bool equals(const std::shared_ptr<const RaftStateSnapshot> &rhs) const {
    return equals(*rhs.get());
  }
};

using RaftStateSnapshotPtr = std::shared_ptr<const RaftStateSnapshot>;

//------------------------------------------------------------------------------
// This class describes the current, authoritative state of raft, and must be
// bulletproof when accessed concurrently.
//------------------------------------------------------------------------------
class RaftState {
public:
  RaftState(RaftJournal &journal, const RaftServer &me);
  DISALLOW_COPY_AND_ASSIGN(RaftState);

  bool observed(RaftTerm term, const RaftServer &leader);
  bool grantVote(RaftTerm term, const RaftServer &vote);
  bool ascend(RaftTerm term);
  bool becomeCandidate(RaftTerm term);
  bool dropOut(RaftTerm term);

  void shutdown();
  bool inShutdown();

  void wait(const std::chrono::milliseconds &t);
  void wait_until(const std::chrono::steady_clock::time_point &t);

  RaftTerm getCurrentTerm();
  RaftStateSnapshotPtr getSnapshot();
  RaftServer getMyself();
  std::vector<RaftServer> getNodes();
  RaftClusterID getClusterID();

  //------------------------------------------------------------------------------
  // Test if the given snapshot is the same as the current one.
  //------------------------------------------------------------------------------
  bool isSnapshotCurrent(const RaftStateSnapshot *ptr) const {
    return ptr == currentSnapshot.get();
  }

  static RaftServer BLOCKED_VOTE;
private:
  RaftJournal &journal;

  std::mutex update;
  std::condition_variable notifier;

  std::atomic<RaftTerm> term;
  std::atomic<RaftStatus> status;
  RaftServer leader;
  RaftServer votedFor;
  LogIndex leadershipMarker;
  const RaftServer myself;

  //------------------------------------------------------------------------------
  // A shared pointer to the current state snapshot.
  //
  // Do not even _think_ of modifying the underlying object. :) The pointer
  // has to be updated atomically with a new object - from a single point in
  // time and onwards, getSnapshot() starts returning a pointer to the new
  // object, but old snapshots remain unchanged and valid.
  //------------------------------------------------------------------------------
  std::shared_ptr<const RaftStateSnapshot> currentSnapshot;

  void updateSnapshot();
  void updateJournal();
  void declareEvent(RaftTerm observedTerm, const RaftServer &observedLeader);
  void updateStatus(RaftStatus newstatus);
};

}

#endif
