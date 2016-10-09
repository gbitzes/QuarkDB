// ----------------------------------------------------------------------
// File: raft.cc
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

#include "raft/Raft.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftTalker.hh"
#include "Tunnel.hh"
#include "Poller.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())

class Raft_Replicator : public TestCluster3Nodes {};
class Raft_Voting : public TestCluster3Nodes {};
class tRaft : public TestCluster3Nodes {};

TEST_F(Raft_Replicator, no_replication_on_myself) {
  ASSERT_TRUE(state()->observed(2, {}));
  ASSERT_TRUE(state()->becomeCandidate(2));
  ASSERT_TRUE(state()->ascend(2));
  ASSERT_THROW(replicator()->launch(myself(), state()->getSnapshot()), FatalException);
}

TEST_F(Raft_Replicator, only_leader_can_launch_replicator) {
  ASSERT_THROW(replicator()->launch(nodes()[1], state()->getSnapshot()), FatalException);
}

TEST_F(Raft_Replicator, verify_sane_snapshot_term) {
  ASSERT_TRUE(state()->observed(2, {}));
  ASSERT_TRUE(state()->becomeCandidate(2));
  ASSERT_TRUE(state()->ascend(2));

  // trying to replicate for a term in the future
  RaftStateSnapshot snapshot = state()->getSnapshot();
  snapshot.term = 3;
  ASSERT_THROW(replicator()->launch(nodes()[1], snapshot), FatalException);

  // stale term - this can naturally happen, so it is not an exception
  ASSERT_TRUE(state()->observed(4, {}));
  ASSERT_FALSE(replicator()->launch(nodes()[1], snapshot));
}

TEST_F(Raft_Replicator, do_simple_replication) {
  // node #0 will replicate its log to node #1
  ASSERT_TRUE(state(0)->observed(2, {}));
  ASSERT_TRUE(state(0)->becomeCandidate(2));
  ASSERT_TRUE(state(0)->ascend(2));

  // add an inconsistent journal entry to #1, just for fun
  ASSERT_TRUE(journal(1)->append(1, 0, make_req("supposed", "to", "be", "removed")));

  ASSERT_EQ(state(1)->getCurrentTerm(), 0);

  // activate poller for #1
  poller(1);

  // launch!
  std::this_thread::sleep_for(std::chrono::milliseconds(2));
  ASSERT_TRUE(replicator(0)->launch(myself(1), state(0)->getSnapshot()));

  // populate #0's journal
  for(size_t i = 0; i < testreqs.size(); i++) {
    ASSERT_TRUE(journal(0)->append(i+1, 2, testreqs[i]));
  }

  // a bit ugly..
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // verify #1 recognized #0 as leader and that replication was successful
  RaftStateSnapshot snapshot = state(1)->getSnapshot();
  ASSERT_EQ(snapshot.term, 2);
  ASSERT_EQ(snapshot.leader, myself(0));
  ASSERT_EQ(journal(1)->getLogSize(), (int64_t) testreqs.size()+1);

  for(size_t i = 0; i < testreqs.size(); i++) {
    RaftEntry entry;
    ASSERT_TRUE(raft(1)->fetch(i+1, entry));
    ASSERT_EQ(entry.term, 2);
    ASSERT_EQ(entry.request, testreqs[i]);
  }
}

TEST_F(tRaft, validate_initial_state) {
  RaftInfo info = raft()->info();
  ASSERT_EQ(info.clusterID, clusterID());
  ASSERT_EQ(info.myself, myself());
  ASSERT_EQ(info.term, 0);
  ASSERT_EQ(info.logSize, 1);

  RaftEntry entry;
  ASSERT_TRUE(raft()->fetch(0, entry));
  ASSERT_EQ(entry.term, 0);
  ASSERT_EQ(entry.request, make_req("UPDATE_RAFT_NODES", serializeNodes(nodes())));
}

TEST_F(tRaft, send_first_heartbeat) {
  // simulate heartbeat from #1 to #0
  RaftAppendEntriesRequest req;
  req.term = 1;
  req.leader = myself(1);
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  RaftAppendEntriesResponse resp = raft()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 1);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 1);
}

TEST_F(tRaft, throw_on_append_entries_from_myself) {
  RaftAppendEntriesRequest req;
  req.term = 2;
  req.leader = myself(0);
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  ASSERT_THROW(raft()->appendEntries(std::move(req)), FatalException);
}

TEST_F(tRaft, add_entries) {
  RaftAppendEntriesRequest req;
  req.term = 2;
  req.leader = myself(1);
  req.prevIndex = 0;
  req.prevTerm = 0;
  req.commitIndex = 0;

  req.entries.push_back(RaftEntry {1, {"set", "qwerty", "123"}});
  req.entries.push_back(RaftEntry {1, {"hset", "abc", "123", "234"}});

  RaftAppendEntriesResponse resp = raft()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 2);
  ASSERT_TRUE(resp.outcome);
  ASSERT_EQ(resp.logSize, 3);

  // previous entry term mismatch, but verify term progressed
  req = {};
  req.term = 3;
  req.leader = myself(1);
  req.prevIndex = 2;
  req.prevTerm = 0;
  req.commitIndex = 0;

  resp = raft()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 3);
  ASSERT_FALSE(resp.outcome);
  ASSERT_EQ(resp.logSize, 3);

  // add three more entries with a different leader, while removing the last
  // entry as inconsistent

  req = {};
  req.term = 5;
  req.leader = myself(2);
  req.prevIndex = 1;
  req.prevTerm = 1;
  req.commitIndex = 1;

  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "a"}});
  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "b"}});
  req.entries.push_back(RaftEntry {3, {"sadd", "myset", "c"}});

  resp = raft()->appendEntries(std::move(req));
  ASSERT_EQ(resp.term, 5);
  ASSERT_TRUE(resp.outcome) << resp.err;
  ASSERT_EQ(resp.logSize, 5);

  RaftEntry entry;
  ASSERT_TRUE(raft()->fetch(2, entry));
  ASSERT_EQ(entry.term, 3);
  ASSERT_EQ(entry.request, make_req("sadd", "myset", "a"));
}

TEST_F(Raft_Voting, throws_with_requestvote_to_myself) {
  RaftVoteRequest req;
  req.term = 1;
  req.candidate = myself();
  req.lastTerm = 0;
  req.lastIndex = 2;

  ASSERT_THROW(raft()->requestVote(req), FatalException);
}

TEST_F(Raft_Voting, no_double_voting_on_same_term) {
  RaftVoteRequest req;
  req.term = 1;
  req.candidate = myself(1);
  req.lastTerm = 0;
  req.lastIndex = 2;

  RaftVoteResponse resp = raft()->requestVote(req);
  ASSERT_TRUE(resp.granted);

  req.candidate = myself(2);
  resp = raft()->requestVote(req);
  ASSERT_FALSE(resp.granted);
}

TEST_F(Raft_Voting, no_votes_for_previous_terms) {
  RaftVoteRequest req;
  req.term = 1;
  req.candidate = myself(1);
  req.lastTerm = 0;
  req.lastIndex = 2;

  RaftVoteResponse resp =  raft()->requestVote(req);
  ASSERT_TRUE(resp.granted);

  req.term = 0;
  resp = raft()->requestVote(req);
  ASSERT_FALSE(resp.granted);
}

TEST_F(Raft_Voting, no_votes_to_outdated_logs) {
  RaftVoteRequest req;
  req.term = 5;
  req.candidate =  myself(1);
  req.lastTerm = 0;
  req.lastIndex = 1;

  RaftVoteResponse resp = raft()->requestVote(req);
  ASSERT_TRUE(resp.granted);

  // add a few requests to the log
  ASSERT_TRUE(journal()->append(1, 3, testreqs[0]));
  ASSERT_TRUE(journal()->append(2, 4, testreqs[1]));
  ASSERT_TRUE(journal()->append(3, 5, testreqs[2]));

  req.term = 6;
  req.candidate = myself(2);
  req.lastTerm = 4;
  req.lastIndex = 30;

  resp = raft()->requestVote(req);
  ASSERT_FALSE(resp.granted);

  req.lastTerm = 5;
  req.lastIndex = 2;

  resp = raft()->requestVote(req);
  ASSERT_FALSE(resp.granted);

  req.lastIndex = 4;
  resp = raft()->requestVote(req);
  ASSERT_TRUE(resp.granted);
}
