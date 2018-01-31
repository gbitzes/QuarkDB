// ----------------------------------------------------------------------
// File: RaftTrimmer.hh
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

#ifndef __QUARKDB_RAFT_TRIMMER_H__
#define __QUARKDB_RAFT_TRIMMER_H__

#include "../utils/AssistedThread.hh"
#include <atomic>
#include <limits>
#include "RaftCommon.hh"

namespace quarkdb {

class RaftTrimmer;

// Not thread safe to call enforce or lift from multiple threads!
// But calling getPreservationIndex is OK, even during calling lift, enforce.
class RaftTrimmingBlock {
public:
  // No block by default.
  RaftTrimmingBlock(RaftTrimmer &trimmer,
    LogIndex preservationLimit = std::numeric_limits<LogIndex>::max());
  ~RaftTrimmingBlock();

  // Convenience function
  // Forwards to enforce(std::numeric_limits<LogIndex>::max())
  void lift();

  // preserveLimit = 0: Block any and all trimming activity.
  // Otherwise: Ensure all entries starting from preserveLimit are spared.
  // This obviously assumes preserveLimit has not been trimmed already. :)
  // We can't do magic. If that's the case, only remaining entries above
  // preserveLimit are spared.
  void enforce(LogIndex preserveLimit = 0);
  LogIndex getPreservationIndex() const;

private:
  std::mutex mtx;
  RaftTrimmer &trimmer;

  // max: block is inactive
  // 0: preserve ALL entries
  std::atomic<LogIndex> preserveIndex;
  bool registered {false};
};

class RaftJournal; class RaftConfig; class StateMachine;
class RaftTrimmer {
public:
  RaftTrimmer(RaftJournal &journal, RaftConfig &raftConfig, StateMachine &sm);
  void registerChange(RaftTrimmingBlock* block);
private:
  std::mutex mtx;
  std::set<RaftTrimmingBlock*> blocks;

  RaftJournal &journal;
  RaftConfig &raftConfig;
  StateMachine &stateMachine;
  AssistedThread mainThread;

  void main(ThreadAssistant &assistant);
  bool canTrimUntil(LogIndex threshold);
  friend class RaftTrimmingBlock;
};

}

#endif
