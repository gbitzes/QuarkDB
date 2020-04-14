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

#include "raft/RaftDirector.hh"
#include "raft/RaftUtils.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftLease.hh"
#include "raft/RaftJournal.hh"
#include "Dispatcher.hh"
#include "StateMachine.hh"

namespace quarkdb {

RaftDirector::RaftDirector(RaftJournal &jour, StateMachine &sm, RaftState &st, RaftLease &ls, RaftCommitTracker &ct, RaftHeartbeatTracker &rht, RaftWriteTracker &wt, ShardDirectory &sharddir, RaftConfig &conf, RaftReplicator &rep, const RaftContactDetails &cd, Publisher &pub)
: journal(jour), stateMachine(sm), state(st), heartbeatTracker(rht), lease(ls), commitTracker(ct), writeTracker(wt), shardDirectory(sharddir), config(conf), replicator(rep), contactDetails(cd), publisher(pub) {
  mainThread = std::thread(&RaftDirector::main, this);
}

RaftDirector::~RaftDirector() {
  state.shutdown();
  mainThread.join();
}

void RaftDirector::main() {
  heartbeatTracker.heartbeat(std::chrono::steady_clock::now());

  while(true) {
    heartbeatTracker.refreshRandomTimeout();
    RaftStateSnapshotPtr snapshot = state.getSnapshot();

    if(snapshot->status == RaftStatus::SHUTDOWN) {
      return;
    }
    else if(snapshot->status == RaftStatus::FOLLOWER) {
      followerLoop(snapshot);
    }
    else if(snapshot->status == RaftStatus::LEADER) {
      leaderLoop(snapshot);
      heartbeatTracker.heartbeat(std::chrono::steady_clock::now());
    }
    else {
      qdb_throw("should never happen");
    }
  }
}

void RaftDirector::leaderLoop(RaftStateSnapshotPtr &snapshot) {
  qdb_assert(snapshot->leader == state.getMyself());
  stateMachine.getRequestCounter().setReportingStatus(true);

  replicator.activate(snapshot);
  while(state.isSnapshotCurrent(snapshot.get())) {
    qdb_assert(checkBasicSanity());

    std::chrono::steady_clock::time_point deadline = lease.getDeadline();
    if(deadline < std::chrono::steady_clock::now()) {
      qdb_event("My leader lease has expired, I no longer control a quorum, stepping down.");
      state.observed(snapshot->term+1, {});
      writeTracker.flushQueues(Formatter::err("unavailable"));
      publisher.purgeListeners(Formatter::err("unavailable"));
      break;
    }

    state.wait_until(deadline);
  }
  replicator.deactivate();
}

bool RaftDirector::checkBasicSanity() {
  // Check that this node's numbers look reasonable before attempting an election.
  // In the unlikely scenario that my memory has somehow been corrupted, this will
  // prevent the errors from propagating to unaffected nodes.
  //
  // Not theoretical: There's been a case where last-applied jumped ahead of
  // commit-index by 1024, and we're not quite sure how this could have happened.
  // (cosmic rays ?! )

  LogIndex lastApplied = stateMachine.getLastApplied();
  LogIndex commitIndex = journal.getCommitIndex();
  LogIndex totalSize = journal.getLogSize();

  bool sanity = true;
  if(commitIndex > totalSize) {
    qdb_critical("Something is very wrong with me, commitIndex is ahead of total journal size: " << commitIndex << " vs " << totalSize << ". Journal corruption?");
    sanity = false;
  }

  if(lastApplied > commitIndex) {
    qdb_critical("Something is very wrong with me, lastApplied is ahead of commit index: " << lastApplied << " vs " << commitIndex << ". Journal lost entries?");
    sanity = false;
  }

  return sanity;
}

void RaftDirector::runForLeader() {
  if(!checkBasicSanity()) {
    qdb_warn("Not running for leader because basic sanity check failed, something's wrong with this node.");
    return;
  }

  // If we get vetoed, this ensures we stop election attempts up until the
  // point we receive a fresh heartbeat.
  std::chrono::steady_clock::time_point lastHeartbeat = heartbeatTracker.getLastHeartbeat();

  // don't reuse the snapshot from the main loop,
  // it could have changed in-between
  RaftStateSnapshotPtr snapshot = state.getSnapshot();

  // advance the term by one, become a candidate.
  if(!state.observed(snapshot->term+1, {})) return;
  if(!state.becomeCandidate(snapshot->term+1)) return;

  // prepare vote request
  RaftVoteRequest votereq;
  votereq.term = snapshot->term+1;
  votereq.lastIndex = journal.getLogSize()-1;
  if(!journal.fetch(votereq.lastIndex, votereq.lastTerm).ok()) {
    qdb_critical("Unable to fetch journal entry " << votereq.lastIndex << " when running for leader");
    state.dropOut(snapshot->term+1);
    return;
  }

  ElectionOutcome electionOutcome = RaftElection::perform(votereq, state, lease, contactDetails);

  if(electionOutcome != ElectionOutcome::kElected) {
    state.dropOut(snapshot->term+1);
  }

  if(electionOutcome == ElectionOutcome::kVetoed) {
    lastHeartbeatBeforeVeto = lastHeartbeat;
    qdb_info("Election round for term " << snapshot->term + 1 << " resulted in a veto. This means, the next leader of this cluster cannot be me. Stopping election attempts until I receive a heartbeat.");
  }
}

void RaftDirector::followerLoop(RaftStateSnapshotPtr &snapshot) {
  stateMachine.getRequestCounter().setReportingStatus(false);
  milliseconds randomTimeout = heartbeatTracker.getRandomTimeout();
  while(true) {
    RaftStateSnapshotPtr now = state.getSnapshot();
    if(snapshot->term != now->term || snapshot->status != now->status) return;

    writeTracker.flushQueues(Formatter::err("unavailable"));
    publisher.purgeListeners(Formatter::err("unavailable"));
    state.wait(randomTimeout);

    if(heartbeatTracker.getLastHeartbeat() == lastHeartbeatBeforeVeto) {
      // I've been vetoed during my last election attempt, and no heartbeat has
      // appeared since then.
      //
      // It could be a network connectivity issue, where I'm able to establish
      // TCP connections to other nodes (and thus disrupt them), but they cannot
      // send me heartbeats.
      //
      // It could also be that I'm not a full member of this cluster, but I
      // don't know it yet, and I'm being disruptive to the other nodes.
      //
      // Since a veto means the next cluster leader cannot be me, completely
      // abstain from starting elections until we receive a heartbeat.
    }
    else if(heartbeatTracker.timeout(std::chrono::steady_clock::now())) {
      RaftMembership membership = journal.getMembership();

      if(membership.inLimbo()) {
        qdb_warn("This node is in limbo: I don't know who the nodes of this cluster are, and I am not receiving heartbeats. Run quarkdb-add-observer on the current leader to add me to the cluster.");
      }
      else if(contains(membership.nodes, state.getMyself())) {
        qdb_event(state.getMyself().toString() <<  ": TIMEOUT after " << randomTimeout.count() << "ms, I am not receiving heartbeats. Attempting to start election.");
        runForLeader();
        return;
      }
      else {
        qdb_warn("I am not receiving heartbeats - not running for leader since in membership epoch " << membership.epoch << " I am not a full node. Will keep on waiting. Maybe I am not part of the members? Run 'raft-info' on the current leader to check the current members, and then run 'quarkdb-add-observer' to add me.");
      }
    }
  }
}

}
