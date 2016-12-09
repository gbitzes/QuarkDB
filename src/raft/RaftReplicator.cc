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
#include <algorithm>
#include "RaftReplicator.hh"
#include "RaftTalker.hh"
#include "RaftUtils.hh"
#include <dirent.h>
#include <fstream>

using namespace quarkdb;

RaftReplicator::RaftReplicator(RaftJournal &journal_, RocksDB &sm, RaftState &state_, const RaftTimeouts t)
: journal(journal_), stateMachine(sm), state(state_), commitTracker(journal, (journal.getNodes().size()/2)+1), timeouts(t) {

}

RaftReplicator::~RaftReplicator() {
  shutdown = 1;
  while(threadsAlive > 0) {
    journal.notifyWaitingThreads();
  }
  for(std::thread &th : threads) th.join();
}

bool RaftReplicator::buildPayload(LogIndex nextIndex, int64_t payloadLimit,
  std::vector<RedisRequest> &reqs, std::vector<RaftTerm> &terms, int64_t &payloadSize) {

  payloadSize = std::min(payloadLimit, journal.getLogSize() - nextIndex);
  for(int64_t i = nextIndex; i < nextIndex+payloadSize; i++) {
    RaftEntry entry;

    if(!journal.fetch(i, entry).ok()) {
      qdb_critical("could not fetch entry with term " << i << " .. aborting building payload");
      return false;
    }

    reqs.push_back(std::move(entry.request));
    terms.push_back(entry.term);
  }
  return true;
}

static bool retrieve_response(std::future<redisReplyPtr> &fut, RaftAppendEntriesResponse &resp) {
  std::future_status status = fut.wait_for(std::chrono::milliseconds(500));
  if(status != std::future_status::ready) {
    return false;
  }

  redisReplyPtr rep = fut.get();
  if(rep == nullptr) return false;

  if(!RaftParser::appendEntriesResponse(rep, resp)) {
    qdb_critical("cannot parse response from append entries");
    return false;
  }

  return true;
}

static bool is_ok_response(std::future<redisReplyPtr> &fut) {
  std::future_status status = fut.wait_for(std::chrono::milliseconds(500));
  if(status != std::future_status::ready) {
    return false;
  }

  redisReplyPtr rep = fut.get();
  if(rep == nullptr) return false;

  if(rep->type != REDIS_REPLY_STATUS) return false;
  if(std::string(rep->str, rep->len) != "OK") return false;

  return true;
}

static bool resilveringCopyDirectory(const std::string &path, const std::string &prefix, Tunnel &tunnel) {
  qdb_info("Copying directory " << path << " under prefix '" << prefix << "'");

  DIR *dir;
  struct dirent *entry;

  dir = opendir(path.c_str());
  if(!dir) {
    qdb_critical("Unable to open directory " << path << " when resilvering");
    return false;
  }

  entry = readdir(dir);
  if(!entry) {
    qdb_critical("Unable to readdir " << path << " when resilvering");
    return false;
  }

  do {
    if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;

    std::string currentPath = SSTR(path << "/" << entry->d_name);
    std::string currentPrefix;

    if(prefix.empty()) {
      currentPrefix = SSTR(entry->d_name);
    }
    else {
      currentPrefix = SSTR(prefix << "/" << entry->d_name);
    }

    if(entry->d_type == DT_DIR) {
      if(!resilveringCopyDirectory(currentPath, currentPrefix, tunnel)) {
        closedir(dir);
        return false;
      }
    }
    else {
      std::ifstream t(currentPath);
      std::stringstream buffer;
      buffer << t.rdbuf();

      std::future<redisReplyPtr> fut = tunnel.execute({"QUARKDB_RESILVERING_COPY_FILE", currentPrefix, buffer.str()});
      if(!is_ok_response(fut)) return false;
    }
  } while( (entry = readdir(dir)));

  return true;
}

static bool cancelResilvering(Tunnel &tunnel) {
  std::future<redisReplyPtr> fut = tunnel.execute({"QUARKDB_CANCEL_RESILVERING"});
  is_ok_response(fut);
  return false;
}

bool RaftReplicator::resilver(const RaftServer &target, const RaftStateSnapshot &snapshot) {
  qdb_critical("Attempting to automatically resilver target " << target.toString());

  Tunnel tunnel(target.hostname, target.port);
  std::future<redisReplyPtr> fut = tunnel.execute({"QUARKDB_START_RESILVERING"});
  if(!is_ok_response(fut)) return cancelResilvering(tunnel);

  std::string checkpointPath = SSTR(chopPath(journal.getDBPath()) << "/tmp/checkpoints-for-" << target.toString());
  system(SSTR("rm -rf " << checkpointPath).c_str());

  std::string journalCheckpoint  = SSTR(checkpointPath << "/raft-journal");
  std::string smCheckpoint = SSTR(checkpointPath << "/state-machine");

  std::string err;
  if(!mkpath(journalCheckpoint, 0755, err)) {
    qdb_critical(err);
    return cancelResilvering(tunnel);
  }

  rocksdb::Status st = journal.checkpoint(journalCheckpoint);
  if(!st.ok()) {
    qdb_critical("cannot create journal checkpoint to perform resilvering in " << journalCheckpoint << ": " << st.ToString());
    return cancelResilvering(tunnel);
  }

  st = stateMachine.checkpoint(smCheckpoint);
  if(!st.ok()) {
    qdb_critical("cannot create state machine checkpoint to perform resilvering in " << smCheckpoint << ": " << st.ToString());
    return cancelResilvering(tunnel);
  }

  if(!resilveringCopyDirectory(checkpointPath, "", tunnel)) return cancelResilvering(tunnel);
  fut = tunnel.execute({"QUARKDB_FINISH_RESILVERING"});
  return is_ok_response(fut);
}

void RaftReplicator::tracker(const RaftServer &target, const RaftStateSnapshot &snapshot) {
  ScopedAdder<int64_t> adder(threadsAlive);
  RaftTalker talker(target, journal.getClusterID());
  LogIndex nextIndex = journal.getLogSize();

  RaftMatchIndexTracker matchIndex;
  std::vector<RaftServer> nodes = journal.getNodes();
  if(contains(nodes, target)) {
    // not an observer, must keep track of its matchIndex
    matchIndex.reset(commitTracker, target);
  }

  bool online = false;
  int64_t payloadLimit = 1;

  bool needResilvering = false;
  while(shutdown == 0 && snapshot.term == state.getCurrentTerm() && !state.inShutdown()) {
    if(nextIndex <= 0) qdb_throw("nextIndex has invalid value: " << nextIndex);
    if(nextIndex <= journal.getLogStart()) nextIndex = journal.getLogSize();

    RaftTerm prevTerm;
    if(!journal.fetch(nextIndex-1, prevTerm).ok()) {
      qdb_critical("unable to fetch log entry " << nextIndex-1 << " when tracking " << target.toString() << ". My log start: " << journal.getLogStart());
      state.wait(timeouts.getHeartbeatInterval());
      continue;
    }

    std::vector<RedisRequest> reqs;
    std::vector<RaftTerm> terms;

    int64_t payloadSize = 0;
    if(!buildPayload(nextIndex, payloadLimit, reqs, terms, payloadSize)) {
      state.wait(timeouts.getHeartbeatInterval());
      continue;
    }

    std::future<redisReplyPtr> fut = talker.appendEntries(snapshot.term, state.getMyself(), nextIndex-1, prevTerm, journal.getCommitIndex(), reqs, terms);
    RaftAppendEntriesResponse resp;

    // Check: Is the target even online?
    if(!retrieve_response(fut, resp)) {
      if(online) {
        payloadLimit = 1;
        qdb_event("Replication target " << target.toString() << " went offline.");
        online = false;
      }

      goto nextRound;
    }

    if(!online) {
      // Print an event if the target just came back online
      online = true;
      qdb_event("Replication target " << target.toString() << " came back online. Outcome: " << resp.outcome << ", logsize: " << resp.logSize);
    }

    state.observed(resp.term, {});
    // Check: Does the target need resilvering?
    if(resp.logSize <= journal.getLogStart()) {
      nextIndex = journal.getLogSize();

      if(!needResilvering) {
        qdb_critical("Unable to perform replication on " << target.toString() << ", it's too far behind (its logsize: " << resp.logSize << ") and my journal starts at " << journal.getLogStart() << ". The target node must be resilvered.");
        needResilvering = true;
        payloadLimit = 1;
        resilver(target, snapshot);
      }

      goto nextRound;
    }

    needResilvering = false;

    // Check: Is my current view of the target's journal correct? (nextIndex)
    if(!resp.outcome) {
      // never try to touch entry #0
      if(nextIndex >= 2 && nextIndex <= resp.logSize) {
        // There are journal inconsistencies. Move back a step to remove a single
        // inconsistent entry in the next round.
        nextIndex--;
      } else if(resp.logSize > 0) {
        // Our nextIndex is outdated, update
        nextIndex = resp.logSize;
      }

      goto nextRound;
    }

    // All checks have passed
    if(nextIndex+payloadSize != resp.logSize) {
      qdb_warn("mismatch in expected logSize. nextIndex = " << nextIndex << ", payloadSize = " << payloadSize << ", logSize: " << resp.logSize);
    }

    matchIndex.update(resp.logSize-1);
    nextIndex = resp.logSize;
    if(payloadLimit < 1024) {
      payloadLimit *= 2;
    }

nextRound:
    if(!online || needResilvering) {
      state.wait(timeouts.getHeartbeatInterval());
    }
    else if(online && nextIndex >= journal.getLogSize()) {
      journal.waitForUpdates(nextIndex, timeouts.getHeartbeatInterval());
    }
    else {
      // don't wait, fire next round of updates
    }
  }
  qdb_event("Shutting down replicator tracker for " << target.toString());
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

  if(current.status != RaftStatus::LEADER && current.status != RaftStatus::SHUTDOWN) {
    qdb_throw("bug, attempted to initiate replication for a term in which I'm not a leader");
  }

  std::lock_guard<std::mutex> lock(threadsMutex);
  threads.emplace_back(&RaftReplicator::tracker, this, target, snapshot);
  return true;
}
