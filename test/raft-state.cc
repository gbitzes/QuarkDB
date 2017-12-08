// ----------------------------------------------------------------------
// File: raft-state.cc
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

#include "raft/RaftState.hh"
#include "raft/RaftJournal.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())

class Raft_State : public ::testing::Test {
protected:
  virtual void SetUp() {
    nodes.emplace_back("server1", 7776);
    nodes.emplace_back("server2", 7777);
    nodes.emplace_back("server3", 7778);
    RaftJournal::ObliterateAndReinitializeJournal(dbpath, clusterID, nodes);
  }

  virtual void TearDown() { }

  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;
  std::string dbpath = "/tmp/raft-journal";
  RaftClusterID clusterID = "55cd595d-7306-4971-b92c-4b9ba5930d40";

  RedisRequest req;
  RedisRequest req2;
  RaftServer srv;
  RaftTerm term;
  RaftServer myself = {"server2", 7777};
};

TEST_F(Raft_State, T1) {
{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  ASSERT_EQ(state.getCurrentTerm(), 0);
  ASSERT_TRUE(state.observed(1, {}));
  ASSERT_FALSE(state.observed(0, {}));
  ASSERT_EQ(myself, state.getMyself());

  RaftStateSnapshot snapshot = {1, RaftStatus::FOLLOWER, {}, {}, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_FALSE(state.observed(0, {"server1", 1234}));
  ASSERT_TRUE(state.observed(1, {"server1", 1234}));
  ASSERT_FALSE(state.observed(1, {"server1", 1234}));
  ASSERT_FALSE(state.observed(1, {"server1", 321}));

  // i've already recognized a leader
  ASSERT_FALSE(state.grantVote(1, {"server1", 7776} ));
  ASSERT_FALSE(state.grantVote(1, {"server2", 7778} ));

  ASSERT_TRUE(state.observed(2, {}));
  ASSERT_TRUE(state.grantVote(2, {"server1", 7778}));

  // cannot vote again
  ASSERT_FALSE(state.grantVote(2, {"server2", 7778}));
  ASSERT_FALSE(state.ascend(2)); // not a candidate, plus have recognized leader

  ASSERT_TRUE(state.observed(3, RaftServer{}));
  ASSERT_TRUE(state.becomeCandidate(3));

  snapshot = {3, RaftStatus::CANDIDATE, {}, myself, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  // drop out from candidacy, make sure I can't try again in the same term
  ASSERT_TRUE(state.dropOut(3));
  snapshot = {3, RaftStatus::FOLLOWER, {}, myself, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));
  ASSERT_FALSE(state.becomeCandidate(3));

  // observed new term, no longer a candidate
  ASSERT_TRUE(state.observed(4, {}));
  snapshot = {4, RaftStatus::FOLLOWER, {}, {}, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_TRUE(state.observed(4, nodes[0]));
  snapshot.leader = nodes[0];
  snapshot.votedFor = RaftState::BLOCKED_VOTE;
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_FALSE(state.becomeCandidate(4)); // already recognized a leader
  ASSERT_FALSE(state.becomeCandidate(3));
  ASSERT_FALSE(state.becomeCandidate(5));

  ASSERT_TRUE(state.observed(5, {}));
  ASSERT_TRUE(state.becomeCandidate(5));
  ASSERT_TRUE(state.ascend(5));

  snapshot = {5, RaftStatus::LEADER, myself, myself, 1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));
  ASSERT_FALSE(state.dropOut(5));
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_TRUE(state.observed(6, nodes[0]));
  snapshot = {6, RaftStatus::FOLLOWER, nodes[0], RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_FALSE(state.inShutdown());
  state.shutdown();
  ASSERT_FALSE(state.observed(200, nodes[0]));
  ASSERT_TRUE(state.inShutdown());
}

{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  RaftStateSnapshot snapshot = {6, RaftStatus::FOLLOWER, {}, RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  // let's erase ourselves from the cluster..
  nodes.erase(nodes.begin()+1);

  std::string err;
  ASSERT_TRUE(journal.removeMember(6, myself, err));
  ASSERT_TRUE(journal.setCommitIndex(2));

  snapshot = {6, RaftStatus::FOLLOWER, {}, RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_TRUE(state.observed(6, nodes[0]));
  snapshot = {6, RaftStatus::FOLLOWER, nodes[0], RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  ASSERT_TRUE(state.observed(7, {}));
  snapshot = {7, RaftStatus::FOLLOWER, {}, {}, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  // cannot become candidate, not part of the cluster
  ASSERT_FALSE(state.becomeCandidate(7));
  ASSERT_FALSE(state.ascend(7));

  // re-enter the cluster as an observer
  nodes.push_back(myself);
  ASSERT_TRUE(journal.addObserver(7, myself, err));
  ASSERT_TRUE(journal.setCommitIndex(3));

  // still cannot call election, not a full node
  ASSERT_FALSE(state.becomeCandidate(7));
  ASSERT_FALSE(state.ascend(7));

  // become full-node
  ASSERT_TRUE(journal.promoteObserver(7, myself, err));
  ASSERT_TRUE(journal.setCommitIndex(4));

  ASSERT_TRUE(state.observed(7, nodes[0]));

  snapshot = {7, RaftStatus::FOLLOWER, nodes[0], RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

  // push two changes to the log
  // mark the first as applied, the other as committed
  ASSERT_TRUE(journal.append(5, RaftEntry(7, "set", "qwerty", "asdf")));
  ASSERT_TRUE(journal.append(6, RaftEntry(7, "set", "1234", "9876")));
  ASSERT_TRUE(journal.setCommitIndex(4));
  ASSERT_FALSE(journal.setCommitIndex(0));
  ASSERT_THROW(journal.setCommitIndex(7), FatalException);
  ASSERT_TRUE(journal.setCommitIndex(6));

  // exit again..
  ASSERT_TRUE(journal.removeMember(7, nodes[2], err));
  nodes.erase(nodes.begin()+2);
  snapshot = {7, RaftStatus::FOLLOWER, nodes[0], RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));
}
{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  RaftStateSnapshot snapshot = {7, RaftStatus::FOLLOWER, {}, RaftState::BLOCKED_VOTE, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));
  ASSERT_EQ(journal.getCurrentTerm(), 7);
  ASSERT_EQ(journal.getVotedFor(), RaftState::BLOCKED_VOTE);

  // verify we remember commit index
  ASSERT_EQ(journal.getCommitIndex(), 6);

  // re-enter cluster
  nodes.push_back(myself);

  ASSERT_TRUE(journal.setCommitIndex(7));
  std::string err;
  ASSERT_TRUE(journal.addObserver(7, myself, err));
  ASSERT_TRUE(journal.setCommitIndex(8));
  ASSERT_TRUE(journal.promoteObserver(7, myself, err));

  // become leader
  ASSERT_TRUE(state.observed(8, {}));
  ASSERT_TRUE(state.becomeCandidate(8));
  ASSERT_TRUE(state.ascend(8));

  snapshot = {8, RaftStatus::LEADER, myself, myself, 10};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));

}
{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  RaftStateSnapshot snapshot = {8, RaftStatus::FOLLOWER, {}, myself, -1};
  ASSERT_TRUE(snapshot.equals(state.getSnapshot()));
}

}
