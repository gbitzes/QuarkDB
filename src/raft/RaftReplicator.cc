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
#include "RaftParser.hh"

using namespace quarkdb;

RaftReplicator::RaftReplicator(RaftJournal &journal_, RaftState &state_)
: journal(journal_), state(state_) {

}

RaftReplicator::~RaftReplicator() {
  shutdown = 1;
  while(threadsAlive > 0) {
    journal.notifyWaitingThreads();
  }
}

bool RaftReplicator::buildPayload(LogIndex nextIndex, size_t messageLength,
  std::vector<RedisRequest> &reqs, std::vector<RaftTerm> &terms) {

  int64_t length = std::min( (int64_t) messageLength, journal.getLogSize() - nextIndex);
  for(int64_t i = nextIndex; i < nextIndex+length; i++) {
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
  std::this_thread::sleep_for(std::chrono::milliseconds(4));

  LogIndex nextIndex = journal.getLogSize();

  bool online = false;
  size_t messageLength = 1;

  while(shutdown == 0 && snapshot.term == state.getCurrentTerm()) {
    RaftTerm prevTerm;
    RedisRequest tmp;

    journal.fetch(nextIndex-1, prevTerm, tmp);

    std::vector<RedisRequest> reqs;
    std::vector<RaftTerm> terms;

    if(!buildPayload(nextIndex, messageLength, reqs, terms)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
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
        if(nextIndex+(int64_t)messageLength != resp.logSize) {
          qdb_critical("mismatch in expected logSize. nextIndex = " << nextIndex << ", messageLength = " << messageLength << ", logSize: " << resp.logSize);
        }

        nextIndex = resp.logSize;
        if(messageLength < 1024) {
          messageLength *= 2;
        }
      }
    }
    else if(online) {
        messageLength = 1;
        qdb_event("Replication target " << target.toString() << " went offline.");
        online = false;
    }

    if(!online) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    else if(online && nextIndex >= journal.getLogSize()) {
      journal.waitForUpdates(nextIndex, std::chrono::milliseconds(500));
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
