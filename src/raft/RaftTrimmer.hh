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

namespace quarkdb {

class RaftTrimmer;

class RaftTrimmingBlock {
public:
  RaftTrimmingBlock(RaftTrimmer &trimmer, bool enabled);
  ~RaftTrimmingBlock();

  void lift();
  void enforce();
  void reset(bool newval);

private:
  RaftTrimmer &trimmer;
  bool enabled;
};

class RaftJournal; class RaftConfig; class StateMachine;
class RaftTrimmer {
public:
  RaftTrimmer(RaftJournal &journal, RaftConfig &raftConfig, StateMachine &sm);

  void block();
  void unblock();

private:
  std::mutex mtx;
  std::atomic<int64_t> blocksActive = {0};

  RaftJournal &journal;
  RaftConfig &raftConfig;
  StateMachine &stateMachine;
  AssistedThread mainThread;

  void main(ThreadAssistant &assistant);
};

}

#endif
