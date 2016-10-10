// ----------------------------------------------------------------------
// File: RaftReplicator.hh
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

#ifndef __QUARKDB_RAFT_REPLICATOR_H__
#define __QUARKDB_RAFT_REPLICATOR_H__

#include "../RocksDB.hh"
#include "RaftJournal.hh"
#include "RaftState.hh"
#include "RaftTimeouts.hh"
#include <mutex>

namespace quarkdb {

//------------------------------------------------------------------------------
// A class that is given a number of target raft machine of the cluster, and
// ensures that their journals match my own.
//------------------------------------------------------------------------------
class RaftReplicator {
public:
  RaftReplicator(RaftJournal &journal, RaftState &state, const RaftTimeouts t = defaultTimeouts);
  ~RaftReplicator();

  bool launch(const RaftServer &target, const RaftStateSnapshot &snapshot);
  void tracker(const RaftServer &target, const RaftStateSnapshot &snapshot);
private:
  bool buildPayload(LogIndex nextIndex, int64_t payloadLimit,
    std::vector<RedisRequest> &reqs, std::vector<RaftTerm> &terms, int64_t &payloadSize);

  RaftJournal &journal;
  RaftState &state;

  std::atomic<int64_t> threadsAlive {0};
  std::atomic<bool> shutdown {0};

  const RaftTimeouts timeouts;
};

}

#endif
