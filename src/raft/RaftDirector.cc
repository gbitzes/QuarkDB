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
#include "../Dispatcher.hh"
using namespace quarkdb;

RaftDirector::RaftDirector(RaftDispatcher &disp, RaftJournal &jour, RaftState &st, RaftClock &rc)
: dispatcher(disp), journal(jour), state(st), raftClock(rc) {
  mainThread = std::thread(&RaftDirector::main, this);
  commitApplier = std::thread(&RaftDirector::applyCommits, this);
  journalTrimmer = std::thread(&RaftDirector::trimJournal, this);
}

RaftDirector::~RaftDirector() {
  state.shutdown();
  journalTrimmer.join();
  journal.notifyWaitingThreads();
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

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

void RaftDirector::applyCommits() {
  LogIndex commitIndex = journal.getCommitIndex(); // local cached value
  while(journal.waitForCommits(commitIndex)) {
    if(state.inShutdown()) break;

    commitIndex = journal.getCommitIndex();
    dispatcher.applyCommits(commitIndex);
  }
}

void RaftDirector::main() {
  raftClock.heartbeat();
  while(true) {
    qdb_info("Random timeout refresh: " << raftClock.refreshRandomTimeout().count() << "ms");
    RaftStateSnapshot snapshot = state.getSnapshot();

    if(snapshot.status == RaftStatus::SHUTDOWN) {
      return;
    }
    else if(snapshot.status == RaftStatus::FOLLOWER) {
      actAsFollower(snapshot);
    }
    else if(snapshot.status == RaftStatus::LEADER) {

      qdb_info("Starting replicator");
      RaftReplicator replicator(journal, state, raftClock.getTimeouts());
      for(const RaftServer& srv : state.getNodes()) {
        if(srv != state.getMyself()) {
          replicator.launch(srv, snapshot);
        }
      }

      while(snapshot.term == state.getCurrentTerm() && state.getSnapshot().status == RaftStatus::LEADER) {
        state.wait(raftClock.getTimeouts().getHeartbeatInterval());
      }
    }
    else if(snapshot.status == RaftStatus::OBSERVER) {
      state.wait(raftClock.getRandomTimeout());
    }
    else if(snapshot.status == RaftStatus::CANDIDATE) {
      qdb_throw("should never happen");
    }
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

  if(!RaftElection::perform(votereq, state, raftClock.getTimeouts())) {
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
      qdb_event(state.getMyself().toString() <<  ": TIMEOUT after " << randomTimeout.count() << "ms, I am not receiving heartbeats. Attempting to start election.");
      runForLeader();
      return;
    }
  }
}
