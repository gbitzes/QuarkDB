// ----------------------------------------------------------------------
// File: RaftCommitTracker.hh
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

#ifndef __QUARKDB_RAFT_COMMIT_TRACKER_H__
#define __QUARKDB_RAFT_COMMIT_TRACKER_H__

#include <mutex>
#include "../Common.hh"
#include "RaftState.hh"

namespace quarkdb {


//------------------------------------------------------------------------------
// The following mechanism might look more complicated than it needs to be, but
// offers the following nice properties:
//
// RaftMatchIndexTracker can be used to track a single node, and de-registers
// itself automatically when deleted, RAII-style.
//
// It's very cheap to update a match index: no map lookups.
//------------------------------------------------------------------------------

class RaftCommitTracker;

class RaftMatchIndexTracker {
public:
  RaftMatchIndexTracker(RaftCommitTracker &tr, const RaftServer &srv);
  RaftMatchIndexTracker();
  ~RaftMatchIndexTracker();

  DISALLOW_COPY_AND_ASSIGN(RaftMatchIndexTracker);

  void update(LogIndex newMatchIndex);
  void reset(RaftCommitTracker &tr, const RaftServer &srv);
  void reset();
private:
  RaftCommitTracker *tracker = nullptr;
  std::map<RaftServer, LogIndex>::iterator it;
  friend class RaftCommitTracker;
};

class RaftCommitTracker {
public:
  RaftCommitTracker(RaftJournal &journal, size_t quorum);
  ~RaftCommitTracker();

  DISALLOW_COPY_AND_ASSIGN(RaftCommitTracker);
  std::map<RaftServer, LogIndex>::iterator registration(const RaftServer &srv);
  void updateQuorum(size_t newQuorum);
private:
  std::mutex mtx;

  RaftJournal &journal;
  size_t quorum;

  std::map<RaftServer, LogIndex> matchIndex;
  LogIndex commitIndex = 0;


  void deregister(RaftMatchIndexTracker &tracker);
  void updated(LogIndex val);
  void recalculateCommitIndex();
  friend class RaftMatchIndexTracker;
};
}
#endif
