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

#ifndef QUARKDB_RAFT_COMMIT_TRACKER_HH
#define QUARKDB_RAFT_COMMIT_TRACKER_HH

#include "utils/AssistedThread.hh"
#include "Common.hh"
#include <mutex>

namespace quarkdb {

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------
class RaftState; class RaftCommitTracker; class RaftJournal;

class RaftMatchIndexTracker {
public:
  RaftMatchIndexTracker(RaftCommitTracker &tr, const RaftServer &srv);

  DISALLOW_COPY_AND_ASSIGN(RaftMatchIndexTracker);

  void update(LogIndex newMatchIndex);
  LogIndex get() { return matchIndex; }
private:
  RaftCommitTracker& tracker;
  const RaftServer server;
  std::atomic<LogIndex> matchIndex {0};
};

class RaftCommitTracker {
public:
  RaftCommitTracker(RaftJournal &journal);
  ~RaftCommitTracker();
  DISALLOW_COPY_AND_ASSIGN(RaftCommitTracker);

  void updateTargets(const std::vector<RaftServer> &targets);
  RaftMatchIndexTracker& getHandler(const RaftServer &srv);

  // Assumption: No references to index trackers are held when calling this
  void reset();

  // This thread only runs if there's just a single node in the "cluster".
  // In such case, replicator will not drive the process of updating
  // commitIndex, we do it ourselves.
  void runAutoCommit(ThreadAssistant &assistant);
private:
  AssistedThread autoCommitter;
  std::mutex mtx;

  RaftJournal &journal;
  size_t quorumSize;

  std::map<RaftServer, RaftMatchIndexTracker*> registrations;
  std::map<RaftServer, RaftMatchIndexTracker*> targets;
  std::vector<LogIndex> matchIndexes;

  LogIndex commitIndex = 0;
  bool commitIndexLagging = false;

  void updated(LogIndex val);
  void recalculateCommitIndex();
  RaftMatchIndexTracker& getHandlerInternal(const RaftServer &srv);
  void updateCommitIndex(LogIndex newCommitIndex);
  friend class RaftMatchIndexTracker;
};
}
#endif
