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

#include "../Common.hh"
#include "RaftCommon.hh"
#include "RaftJournal.hh"
#include <mutex>

namespace quarkdb {



struct RaftStateSnapshot {
  RaftTerm term;
  RaftStatus status;
  RaftServer leader;
  RaftServer votedFor;

  bool operator==(const RaftStateSnapshot& rhs) const {
    return term == rhs.term && status == rhs.status && leader == rhs.leader && votedFor == rhs.votedFor;
  }
};

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
  RaftStateSnapshot getSnapshot();
  RaftServer getMyself();
  std::vector<RaftServer> getNodes();
  RaftClusterID getClusterID();

  static RaftServer BLOCKED_VOTE;
private:
  RaftJournal &journal;

  std::mutex update;
  std::condition_variable notifier;

  std::atomic<RaftTerm> term;
  std::atomic<RaftStatus> status;
  RaftServer leader;
  RaftServer votedFor;
  const RaftServer myself;

  void updateJournal();
  void declareEvent(RaftTerm observedTerm, const RaftServer &observedLeader);
  void updateStatus(RaftStatus newstatus);
};

}

#endif
