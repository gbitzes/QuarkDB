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
#include "RaftResilverer.hh"
#include "RaftConfig.hh"
#include "RaftState.hh"
#include "RaftJournal.hh"
#include "RaftCommitTracker.hh"
#include "RaftTimeouts.hh"
#include "RaftLease.hh"
#include "../utils/FileUtils.hh"
#include <dirent.h>
#include <fstream>

using namespace quarkdb;

RaftReplicator::RaftReplicator(RaftJournal &journal_, RaftState &state_, RaftLease &lease_, RaftCommitTracker &ct, RaftTrimmer &trim, ShardDirectory &sharddir, RaftConfig &conf, const RaftTimeouts t)
: journal(journal_), state(state_), lease(lease_), commitTracker(ct), trimmer(trim), shardDirectory(sharddir), config(conf), timeouts(t) {

}

RaftReplicator::~RaftReplicator() {
  deactivate();
}

RaftReplicaTracker::RaftReplicaTracker(const RaftServer &target_, const RaftStateSnapshot &snapshot_, RaftJournal &journal_, RaftState &state_, RaftLease &lease_, RaftCommitTracker &ct, RaftTrimmer &trim, ShardDirectory &sharddir, RaftConfig &conf, const RaftTimeouts t)
: target(target_), snapshot(snapshot_), journal(journal_),
  state(state_), lease(lease_), commitTracker(ct), trimmer(trim), shardDirectory(sharddir), config(conf), timeouts(t),
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
  heartbeatThread.reset(&RaftReplicaTracker::sendHeartbeats, this);
}

RaftReplicaTracker::~RaftReplicaTracker() {
  shutdown = 1;
  while(running) {
    journal.notifyWaitingThreads();
  }
  if(thread.joinable()) {
    thread.join();
  }

  if(resilverer) {
    delete resilverer;
    resilverer = nullptr;
  }
}

bool RaftReplicaTracker::buildPayload(LogIndex nextIndex, int64_t payloadLimit,
  std::vector<RaftEntry> &entries, int64_t &payloadSize) {

  payloadSize = std::min(payloadLimit, journal.getLogSize() - nextIndex);
  entries.resize(payloadSize);

  for(int64_t i = nextIndex; i < nextIndex+payloadSize; i++) {
    if(!journal.fetch(i, entries[i-nextIndex]).ok()) {
      qdb_critical("could not fetch entry with term " << i << " .. aborting building payload");
      return false;
    }
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

static bool retrieve_heartbeat_reply(std::future<redisReplyPtr> &fut, RaftHeartbeatResponse &resp) {
  std::future_status status = fut.wait_for(std::chrono::milliseconds(500));
  if(status != std::future_status::ready) {
    return false;
  }

  redisReplyPtr rep = fut.get();
  if(rep == nullptr) return false;

  if(!RaftParser::heartbeatResponse(rep, resp)) {
    qdb_critical("cannot parse response from heartbeat");
    return false;
  }

  return true;
}

void RaftReplicaTracker::triggerResilvering() {
  // Check: Already resilvering target?
  if(resilverer && resilverer->getStatus().state == ResilveringState::INPROGRESS) {
    return;
  }

  if(resilverer && resilverer->getStatus().state == ResilveringState::FAILED) {
    qdb_critical("Resilvering attempt for " << target.toString() << " failed: " << resilverer->getStatus().err);
    delete resilverer;
    resilverer = nullptr;

    // Try again during the next round
    return;
  }

  // Start the resilverer
  resilverer = new RaftResilverer(shardDirectory, target, journal.getClusterID(), timeouts, &trimmer);
}

// Go through the pending queue, checking if any responses from the target have
// arrived.
bool RaftReplicaTracker::checkPendingQueue(std::queue<PendingResponse> &inflight) {
  const int64_t pipelineLength = 10;
  while(true) {
    if(inflight.size() == 0) return true;
    if(!is_ready(inflight.front().fut) && inflight.size() <= pipelineLength) return true;

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

  std::queue<PendingResponse> inflight;
  while(shutdown == 0 && snapshot.term == state.getCurrentTerm() && !state.inShutdown()) {
    // Check for pending responses
    if(!checkPendingQueue(inflight)) {
      // Instruct the parent to continue from that point on. No guarantees that
      // this is actually the current logsize of the target, but the parent will
      // figure it out if not.
      return nextIndex;
    }

    RaftTerm prevTerm;
    if(!journal.fetch(nextIndex-1, prevTerm).ok()) {
      qdb_critical("unable to fetch log entry " << nextIndex-1 << " when tracking " << target.toString() << ". My log start: " << journal.getLogStart());
      state.wait(timeouts.getHeartbeatInterval());
      continue;
    }

    std::vector<RaftEntry> entries;

    int64_t payloadSize = 0;
    if(!buildPayload(nextIndex, payloadLimit, entries, payloadSize)) {
      state.wait(timeouts.getHeartbeatInterval());
      continue;
    }

    std::chrono::steady_clock::time_point contact = std::chrono::steady_clock::now();
    inflight.emplace(
      talker.appendEntries(snapshot.term, state.getMyself(), nextIndex-1, prevTerm, journal.getCommitIndex(), entries),
      contact,
      nextIndex,
      payloadSize
    );

    // Assume a positive response from the target, and keep pushing
    // if there are more entries.
    nextIndex += payloadSize;

    updateStatus(true, nextIndex);
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

void RaftReplicaTracker::updateStatus(bool online, LogIndex nextIndex) {
  statusOnline = online;
  statusNextIndex = nextIndex;
}

ReplicaStatus RaftReplicaTracker::getStatus() {
  return { target, statusOnline, statusNextIndex };
}

void RaftReplicaTracker::sendHeartbeats(ThreadAssistant &assistant) {
  RaftTalker talker(target, journal.getClusterID(), timeouts);

  while(!assistant.terminationRequested() && shutdown == 0 && snapshot.term == state.getCurrentTerm() && !state.inShutdown()) {
    std::chrono::steady_clock::time_point contact = std::chrono::steady_clock::now();
    std::future<redisReplyPtr> fut = talker.heartbeat(snapshot.term, state.getMyself());
    RaftHeartbeatResponse resp;

    if(!retrieve_heartbeat_reply(fut, resp)) {
      goto nextRound;
    }

    state.observed(resp.term, {});
    if(snapshot.term < resp.term || !resp.nodeRecognizedAsLeader) continue;
    lastContact.heartbeat(contact);

nextRound:
    state.wait(timeouts.getHeartbeatInterval());
  }
}

void RaftReplicaTracker::main() {
  RaftTalker talker(target, journal.getClusterID(), timeouts);
  LogIndex nextIndex = journal.getLogSize();

  RaftMatchIndexTracker &matchIndex = commitTracker.getHandler(target);
  RaftLastContact &lastContact = lease.getHandler(target);

  bool online = false;
  int64_t payloadLimit = 1;

  bool needResilvering = false;
  while(shutdown == 0 && snapshot.term == state.getCurrentTerm() && !state.inShutdown()) {
    // Target looks pretty stable, start continuous stream
    // if(online && payloadLimit >= 8) {
    //   nextIndex = streamUpdates(talker, nextIndex);
    //   // Something happened when streaming updates, switch back to conservative
    //   // mode and wait for each response
    //   payloadLimit = 1;
    //   continue;
    // }

    if(nextIndex <= 0) qdb_throw("nextIndex has invalid value: " << nextIndex);
    if(nextIndex <= journal.getLogStart()) nextIndex = journal.getLogSize();

    RaftTerm prevTerm;
    if(!journal.fetch(nextIndex-1, prevTerm).ok()) {
      qdb_critical("unable to fetch log entry " << nextIndex-1 << " when tracking " << target.toString() << ". My log start: " << journal.getLogStart());
      state.wait(timeouts.getHeartbeatInterval());
      continue;
    }

    std::vector<RaftEntry> entries;

    int64_t payloadSize = 0;
    if(!buildPayload(nextIndex, payloadLimit, entries, payloadSize)) {
      state.wait(timeouts.getHeartbeatInterval());
      continue;
    }

    std::chrono::steady_clock::time_point contact = std::chrono::steady_clock::now();
    std::future<redisReplyPtr> fut = talker.appendEntries(snapshot.term, state.getMyself(), nextIndex-1, prevTerm, journal.getCommitIndex(), entries);
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
      qdb_event("Replication target " << target.toString() << " came back online. Log size: " << resp.logSize << ", lagging " << (journal.getLogSize() - resp.logSize) << " entries behind me. (approximate)");
    }

    state.observed(resp.term, {});
    if(snapshot.term < resp.term) continue;
    lastContact.heartbeat(contact);

    // Check: Does the target need resilvering?
    if(resp.logSize <= journal.getLogStart()) {
      nextIndex = journal.getLogSize();

      if(!needResilvering) {
        qdb_event("Unable to perform replication on " << target.toString() << ", it's too far behind (its logsize: " << resp.logSize << ") and my journal starts at " << journal.getLogStart() << ".");
        needResilvering = true;
        payloadLimit = 1;
      }

      if(config.getResilveringEnabled()) {
        triggerResilvering();
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
    updateStatus(online, nextIndex);
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

void RaftReplicator::activate(RaftStateSnapshot &snapshot_) {
  std::lock_guard<std::recursive_mutex> lock(mtx);
  qdb_event("Activating replicator for term " << snapshot_.term);

  qdb_assert(targets.empty());
  snapshot = snapshot_;

  reconfigure();
}

void RaftReplicator::deactivate() {
  std::lock_guard<std::recursive_mutex> lock(mtx);
  qdb_event("De-activating replicator");

  for(auto it = targets.begin(); it != targets.end(); it++) {
    delete it->second;
  }
  targets.clear();

  snapshot = {};
}

ReplicationStatus RaftReplicator::getStatus() {
  std::lock_guard<std::recursive_mutex> lock(mtx);

  ReplicationStatus ret;
  for(auto it = targets.begin(); it != targets.end(); it++) {
    ret.addReplica(it->second->getStatus());
  }

  return ret;
}

static std::vector<RaftServer> all_servers_except_myself(const std::vector<RaftServer> &nodes, const RaftServer &myself) {
  std::vector<RaftServer> remaining;
  size_t skipped = 0;

  for(size_t i = 0; i < nodes.size(); i++) {
    if(myself == nodes[i]) {
      if(skipped != 0) qdb_throw("found myself in the nodes list twice");
      skipped++;
      continue;
    }
    remaining.push_back(nodes[i]);
  }

  if(skipped != 1) qdb_throw("unexpected value for 'skipped', got " << skipped << " instead of 1");
  if(remaining.size() != nodes.size()-1) qdb_throw("unexpected size for remaining: " << remaining.size() << " instead of " << nodes.size()-1);
  return remaining;
}

void RaftReplicator::reconfigure() {
  RaftMembership membership = journal.getMembership();
  qdb_info("Reconfiguring replicator for membership epoch " << membership.epoch);

  // Build list of targets
  std::vector<RaftServer> full_nodes = all_servers_except_myself(membership.nodes, state.getMyself());
  std::vector<RaftServer> targets = full_nodes;

  // add observers
  for(const RaftServer& srv : membership.observers) {
    if(srv == state.getMyself()) qdb_throw("found myself in the list of observers, even though I'm leader: " << serializeNodes(membership.observers));
    targets.push_back(srv);
  }

  // reconfigure lease and commit tracker - only take into account full nodes!
  commitTracker.updateTargets(full_nodes);
  lease.updateTargets(full_nodes);

  // now set them
  setTargets(targets);
}

void RaftReplicator::setTargets(const std::vector<RaftServer> &newTargets) {
  std::lock_guard<std::recursive_mutex> lock(mtx);

  // add targets?
  for(size_t i = 0; i < newTargets.size(); i++) {
    if(targets.find(newTargets[i]) == targets.end()) {
      targets[newTargets[i]] = new RaftReplicaTracker(newTargets[i], snapshot, journal, state, lease, commitTracker, trimmer, shardDirectory, config, timeouts);
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
