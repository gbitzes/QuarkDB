// ----------------------------------------------------------------------
// File: RaftReplicator.cc
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

#include <thread>
#include "RaftReplicator.hh"
#include "RaftTalker.hh"
#include "RaftUtils.hh"

using namespace quarkdb;

RaftReplicator::RaftReplicator(RaftJournal &journal_, RaftState &state_, const RaftTimeouts t)
: journal(journal_), state(state_), timeouts(t) {

}

RaftReplicator::~RaftReplicator() {
  shutdown = 1;
  while(threadsAlive > 0) {
    journal.notifyWaitingThreads();
  }
}

bool RaftReplicator::buildPayload(LogIndex nextIndex, int64_t payloadLimit,
  std::vector<RedisRequest> &reqs, std::vector<RaftTerm> &terms, int64_t &payloadSize) {

  payloadSize = std::min(payloadLimit, journal.getLogSize() - nextIndex);
  for(int64_t i = nextIndex; i < nextIndex+payloadSize; i++) {
    RaftTerm entryTerm;
    RedisRequest entry;

    if(!journal.fetch(i, entryTerm, entry).ok()) {
      qdb_critical("could not fetch entry with term " << i << " .. aborting building payload");
      return false;
    }

    reqs.push_back(std::move(entry));
    terms.push_back(entryTerm);
  }
  return true;
}

static bool retrieve_response(std::future<redisReplyPtr> &fut, RaftAppendEntriesResponse &resp) {
  std::future_status status = fut.wait_for(std::chrono::milliseconds(500));
  if(status != std::future_status::ready) {
    return false;
  }

  redisReplyPtr rep = fut.get();
  if(!RaftParser::appendEntriesResponse(rep, resp)) {
    qdb_critical("cannot parse response from append entries");
    return false;
  }

  return true;
}

void RaftReplicator::tracker(const RaftServer &target, const RaftStateSnapshot &snapshot) {
  ScopedAdder<int64_t> adder(threadsAlive);
  RaftTalker talker(target, journal.getClusterID());
  LogIndex nextIndex = journal.getLogSize();

  bool online = false;
  int64_t payloadLimit = 1;
  while(shutdown == 0 && snapshot.term == state.getCurrentTerm()) {
    RaftTerm prevTerm;
    RedisRequest tmp;

    journal.fetch(nextIndex-1, prevTerm, tmp);

    std::vector<RedisRequest> reqs;
    std::vector<RaftTerm> terms;

    int64_t payloadSize = 0;
    if(!buildPayload(nextIndex, payloadLimit, reqs, terms, payloadSize)) {
      std::this_thread::sleep_for(timeouts.getHeartbeatInterval());
      continue;
    }

    std::future<redisReplyPtr> fut = talker.appendEntries(snapshot.term, state.getMyself(), nextIndex-1, prevTerm, 0, reqs, terms);
    RaftAppendEntriesResponse resp;

    if(retrieve_response(fut, resp)) {
      if(!online) {
        online = true;
        qdb_event("Replication target " << target.toString() << " came back online.");
      }

      if(!resp.outcome) {
        if(nextIndex <= resp.logSize) {
          nextIndex--;
        } else if(resp.logSize > 0) {
          nextIndex = resp.logSize;
        }
      }

      if(resp.outcome) {
        if(nextIndex+payloadSize != resp.logSize) {
          qdb_critical("mismatch in expected logSize. nextIndex = " << nextIndex << ", payloadSize = " << payloadSize << ", logSize: " << resp.logSize);
        }

        nextIndex = resp.logSize;
        if(payloadLimit < 1024) {
          payloadLimit *= 2;
        }
      }
    }
    else if(online) {
        payloadLimit = 1;
        qdb_event("Replication target " << target.toString() << " went offline.");
        online = false;
    }

    if(!online) {
      std::this_thread::sleep_for(timeouts.getHeartbeatInterval());
    }
    else if(online && nextIndex >= journal.getLogSize()) {
      journal.waitForUpdates(nextIndex, timeouts.getHeartbeatInterval());
    }
    else {
      // don't wait, fire next round of updates
    }
  }
}

bool RaftReplicator::launch(const RaftServer &target, const RaftStateSnapshot &snapshot) {
  if(target == state.getMyself()) {
    qdb_throw("attempted to run RaftReplicator on myself");
  }

  RaftStateSnapshot current = state.getSnapshot();
  if(snapshot.term > current.term) {
    qdb_throw("bug, a state snapshot has a larger term than the current state");
  }

  if(snapshot.term < current.term) {
    return false;
  }

  if(current.status != RaftStatus::LEADER) {
    qdb_throw("bug, attempted to initiate replication for a term in which I'm not a leader");
  }

  std::thread th(&RaftReplicator::tracker, this, target, snapshot);
  th.detach();
  return true;
}
