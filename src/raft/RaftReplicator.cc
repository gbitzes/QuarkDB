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

RaftReplicator::RaftReplicator(RaftStateSnapshot &snapshot_, RaftJournal &journal_, StateMachine &sm, RaftState &state_, RaftLease &lease_, RaftCommitTracker &ct, const RaftTimeouts t)
: snapshot(snapshot_), journal(journal_), stateMachine(sm), state(state_), lease(lease_), commitTracker(ct), timeouts(t) {

}

RaftReplicator::~RaftReplicator() {
  for(auto it = targets.begin(); it != targets.end(); it++) {
    delete it->second;
  }
}

RaftReplicaTracker::RaftReplicaTracker(const RaftServer &target_, const RaftStateSnapshot &snapshot_, RaftJournal &journal_, StateMachine &sm, RaftState &state_, RaftLease &lease_, RaftCommitTracker &ct, const RaftTimeouts t)
: target(target_), snapshot(snapshot_), journal(journal_), stateMachine(sm),
  state(state_), lease(lease_), commitTracker(ct), timeouts(t),
  matchIndex(commitTracker.getHandler(target)),
  lastContact(lease.getHandler(target))  {
  if(target == state.getMyself()) {
    qdb_throw("attempted to run replication on myself");
  }

  RaftStateSnapshot current = state.getSnapshot();
  if(snapshot.term > current.term) {
    qdb_throw("bug, a state snapshot has a larger term than the current state");
  }

  if(snapshot.term < current.term) {
    return;
  }

  if(current.status != RaftStatus::LEADER && current.status != RaftStatus::SHUTDOWN) {
    qdb_throw("bug, attempted to initiate replication for a term in which I'm not a leader");
  }

  running = true;
  thread = std::thread(&RaftReplicaTracker::main, this);
}

RaftReplicaTracker::~RaftReplicaTracker() {
  shutdown = 1;
  while(running) {
    journal.notifyWaitingThreads();
  }
  if(thread.joinable()) {
    thread.join();
  }
}

bool RaftReplicaTracker::buildPayload(LogIndex nextIndex, int64_t payloadLimit,
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

static bool is_ready(std::future<redisReplyPtr> &fut) {
  return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
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

static bool resilveringCopyDirectory(const std::string &path, const std::string &prefix, qclient::QClient &tunnel) {
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

static bool cancelResilvering(QClient &tunnel) {
  std::future<redisReplyPtr> fut = tunnel.execute({"QUARKDB_CANCEL_RESILVERING"});
  is_ok_response(fut);
  return false;
}

bool RaftReplicaTracker::resilver() {
  qdb_critical("Attempting to automatically resilver target " << target.toString());

  QClient tunnel(target.hostname, target.port);
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

// Go through the pending queue, checking if any responses from the target have
// arrived.
//
// If we're here, it means our target is very stable, so we should be able to
// continuously stream updates without waiting for the replies.
//
// As soon as an error is discovered we return, and let the parent function
// deal with it to stabilize the target once more.
bool RaftReplicaTracker::checkPendingQueue(std::queue<PendingResponse> &inflight) {
  while(true) {
    if(inflight.size() == 0) return true;
    if(!is_ready(inflight.front().fut)) return true;

    PendingResponse item = std::move(inflight.front());
    inflight.pop();

    RaftAppendEntriesResponse resp;
    if(!retrieve_response(item.fut, resp)) return false;
    state.observed(resp.term, {});

    if(!resp.outcome) return false;
    if(resp.term != snapshot.term) return false;

    lastContact.heartbeat(item.sent);
    matchIndex.update(resp.logSize-1);

    if(resp.logSize != item.pushedFrom + item.payloadSize) return false;
  }
  return true;
}

LogIndex RaftReplicaTracker::streamUpdates(RaftTalker &talker, LogIndex firstNextIndex) {

  // If we're here, it means our target is very stable, so we should be able to
  // continuously stream updates without waiting for the replies.
  //
  // As soon as an error is discovered we return, and let the parent function
  // deal with it to stabilize the target once more.

  const int64_t payloadLimit = 200;
  LogIndex nextIndex = firstNextIndex;

  std::queue<PendingResponse> inflight; // Queue<PendingResponse> inflight;
  while(shutdown == 0 && snapshot.term == state.getCurrentTerm() && !state.inShutdown()) {
    // Check for pending responses
    if(!checkPendingQueue(inflight)) {
      // Instruct the parent to continue from that point on. No guarantees that
      // this is actually the current logsize of the target, but the parent will
      // figure it out if not.
      return nextIndex;
      // return inflight.front().pushedFrom;
    }

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

    std::chrono::steady_clock::time_point contact = std::chrono::steady_clock::now();
    inflight.emplace(
      talker.appendEntries(snapshot.term, state.getMyself(), nextIndex-1, prevTerm, journal.getCommitIndex(), reqs, terms),
      contact,
      nextIndex,
      payloadSize
    );

    // Assume a positive response from the target, and keep pushing
    // if there are more entries.
    nextIndex += payloadSize;

    if(nextIndex >= journal.getLogSize()) {
      journal.waitForUpdates(nextIndex, timeouts.getHeartbeatInterval());
    }
    else {
      // fire next round
    }
  }

  // Again, no guarantees this is the actual, current logSize of the target,
  // but the parent will figure it out.
  return nextIndex;
}

void RaftReplicaTracker::main() {
  RaftTalker talker(target, journal.getClusterID());
  LogIndex nextIndex = journal.getLogSize();

  RaftMatchIndexTracker &matchIndex = commitTracker.getHandler(target);
  RaftLastContact &lastContact = lease.getHandler(target);

  bool online = false;
  int64_t payloadLimit = 1;

  bool needResilvering = false;
  while(shutdown == 0 && snapshot.term == state.getCurrentTerm() && !state.inShutdown()) {
    // Target looks pretty stable, start continuous stream
    if(online && payloadLimit >= 8) {
      nextIndex = streamUpdates(talker, nextIndex);
      // Something happened when streaming updates, switch back to conservative
      // mode and wait for each response
      payloadLimit = 1;
      continue;
    }

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

    std::chrono::steady_clock::time_point contact = std::chrono::steady_clock::now();
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
      qdb_event("Replication target " << target.toString() << " came back online. Log size: " << resp.logSize << ", lagging " << (journal.getLogSize() - resp.logSize) << " entries behind mine. (approximate)");
    }

    state.observed(resp.term, {});
    if(snapshot.term < resp.term) continue;
    lastContact.heartbeat(contact);

    // Check: Does the target need resilvering?
    if(resp.logSize <= journal.getLogStart()) {
      nextIndex = journal.getLogSize();

      if(!needResilvering) {
        qdb_critical("Unable to perform replication on " << target.toString() << ", it's too far behind (its logsize: " << resp.logSize << ") and my journal starts at " << journal.getLogStart() << ". The target node must be resilvered.");
        needResilvering = true;
        payloadLimit = 1;
        resilver();
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
      qdb_warn("mismatch in expected logSize. nextIndex = " << nextIndex << ", payloadSize = " << payloadSize << ", logSize: " << resp.logSize << ", resp.term: " << resp.term << ", my term: " << snapshot.term << ", journal size: " << journal.getLogSize());
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
  running = false;
}

void RaftReplicator::setTargets(const std::vector<RaftServer> &newTargets) {
  std::lock_guard<std::mutex> lock(mtx);

  // add targets?
  for(size_t i = 0; i < newTargets.size(); i++) {
    if(targets.find(newTargets[i]) == targets.end()) {
      targets[newTargets[i]] = new RaftReplicaTracker(newTargets[i], snapshot, journal, stateMachine, state, lease, commitTracker, timeouts);
    }
  }

  // remove targets?
  std::vector<RaftServer> todel;

  for(auto it = targets.begin(); it != targets.end(); it++) {
    if(!contains(newTargets, it->first)) {
      todel.push_back(it->first);
    }
  }

  for(size_t i = 0; i < todel.size(); i++) {
    delete targets[todel[i]];
    targets.erase(todel[i]);
  }
}
