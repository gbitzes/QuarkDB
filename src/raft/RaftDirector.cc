// ----------------------------------------------------------------------
// File: RaftDirector.cc
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

#include "RaftDirector.hh"
#include "RaftUtils.hh"
#include "RaftReplicator.hh"
#include "RaftLease.hh"
#include "../Dispatcher.hh"
using namespace quarkdb;

RaftDirector::RaftDirector(RaftDispatcher &disp, RaftJournal &jour, StateMachine &sm, RaftState &st, RaftLease &ls, RaftCommitTracker &ct, RaftClock &rc)
: dispatcher(disp), journal(jour), stateMachine(sm), state(st), raftClock(rc), lease(ls), commitTracker(ct) {
  mainThread = std::thread(&RaftDirector::main, this);
  commitApplier = std::thread(&RaftDirector::applyCommits, this);
  journalTrimmer = std::thread(&RaftDirector::trimJournal, this);
}

RaftDirector::~RaftDirector() {
  state.shutdown();
  journalTrimmer.join();
  while(commitApplierActive) {
    journal.notifyWaitingThreads();
  }
  commitApplier.join();
  mainThread.join();
}

void RaftDirector::trimJournal() {
  while(!state.inShutdown()) {
    LogIndex logSpan = journal.getLogSize() - journal.getLogStart();

    // TODO: make these configurable?
    if(logSpan >= 1000000) {
      journal.trimUntil(journal.getLogStart() + 100000);
    }
    else {
      state.wait(std::chrono::seconds(1));
    }
  }
}

void RaftDirector::applyCommits() {
  LogIndex commitIndex = journal.getCommitIndex(); // local cached value
  while(journal.waitForCommits(commitIndex)) {
    if(state.inShutdown()) break;

    commitIndex = journal.getCommitIndex();
    dispatcher.applyCommits(commitIndex);
  }
  commitApplierActive = false;
}

void RaftDirector::main() {
  raftClock.heartbeat();
  while(true) {
    raftClock.refreshRandomTimeout();
    RaftStateSnapshot snapshot = state.getSnapshot();

    if(snapshot.status == RaftStatus::SHUTDOWN) {
      return;
    }
    else if(snapshot.status == RaftStatus::FOLLOWER) {
      actAsFollower(snapshot);
    }
    else if(snapshot.status == RaftStatus::LEADER) {
      actAsLeader(snapshot);
      raftClock.heartbeat();
    }
    else {
      qdb_throw("should never happen");
    }
  }
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

void RaftDirector::actAsLeader(RaftStateSnapshot &snapshot) {
  RaftMembership membership = journal.getMembership();
  qdb_info("Starting replicator for membership epoch " << membership.epoch);
  if(snapshot.leader != state.getMyself()) qdb_throw("attempted to act as leader, even though snapshot shows a different one");

  std::vector<RaftServer> targets = all_servers_except_myself(membership.nodes, state.getMyself());
  commitTracker.updateTargets(targets);
  lease.updateTargets(targets);

  RaftReplicator replicator(journal, stateMachine, state, lease, commitTracker, raftClock.getTimeouts());
  for(const RaftServer& srv : membership.nodes) {
    if(srv != state.getMyself()) {
      replicator.launch(srv, snapshot);
    }
  }

  for(const RaftServer& srv : membership.observers) {
    if(srv == state.getMyself()) qdb_throw("found myself in the list of observers, even though I'm leader: " << serializeNodes(membership.observers));
    replicator.launch(srv, snapshot);
  }

  while(membership.epoch == journal.getEpoch() &&
        snapshot.term == state.getCurrentTerm() &&
        state.getSnapshot().status == RaftStatus::LEADER) {

    std::chrono::steady_clock::time_point deadline = lease.getDeadline();
    if(deadline < std::chrono::steady_clock::now()) {
      qdb_event("My leader lease has expired, I no longer control a quorum, stepping down.");
      state.observed(snapshot.term+1, {});
      return;
    }

    state.wait_until(deadline);
  }
}

void RaftDirector::runForLeader() {
  // don't reuse the snapshot from the main loop,
  // it could have changed in-between
  RaftStateSnapshot snapshot = state.getSnapshot();

  // advance the term by one, become a candidate.
  if(!state.observed(snapshot.term+1, {})) return;
  if(!state.becomeCandidate(snapshot.term+1)) return;

  // prepare vote request
  RaftVoteRequest votereq;
  votereq.term = snapshot.term+1;
  votereq.lastIndex = journal.getLogSize()-1;
  if(!journal.fetch(votereq.lastIndex, votereq.lastTerm).ok()) {
    qdb_critical("Unable to fetch journal entry " << votereq.lastIndex << " when running for leader");
    state.dropOut(snapshot.term+1);
    return;
  }

  if(!RaftElection::perform(votereq, state, lease, raftClock.getTimeouts())) {
    state.dropOut(snapshot.term+1);
  }
}

void RaftDirector::actAsFollower(RaftStateSnapshot &snapshot) {
  milliseconds randomTimeout = raftClock.getRandomTimeout();
  while(true) {
    RaftStateSnapshot now = state.getSnapshot();
    if(snapshot.term != now.term || snapshot.status != now.status) return;

    state.wait(randomTimeout);
    if(raftClock.timeout()) {
      if(contains(journal.getMembership().nodes, state.getMyself())) {
        qdb_event(state.getMyself().toString() <<  ": TIMEOUT after " << randomTimeout.count() << "ms, I am not receiving heartbeats. Attempting to start election.");
        runForLeader();
        return;
      }
      qdb_warn("I am not receiving heartbeats - not running for leader since in membership epoch " << journal.getEpoch() << " I am not a full node. Will keep on waiting.");
    }
  }
}
