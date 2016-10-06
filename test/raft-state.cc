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

  RaftStateSnapshot snapshot = {1, RaftStatus::FOLLOWER, {}, {} };
  ASSERT_EQ(state.getSnapshot(), snapshot);

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

  snapshot = {3, RaftStatus::CANDIDATE, {}, myself};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  // observed new term, no longer a candidate
  ASSERT_TRUE(state.observed(4, {}));
  snapshot = {4, RaftStatus::FOLLOWER, {}, {}};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  ASSERT_TRUE(state.observed(4, nodes[0]));
  snapshot.leader = nodes[0];
  snapshot.votedFor = RaftState::BLOCKED_VOTE;
  ASSERT_EQ(state.getSnapshot(), snapshot);

  ASSERT_FALSE(state.becomeCandidate(4)); // already recognized a leader
  ASSERT_FALSE(state.becomeCandidate(3));
  ASSERT_FALSE(state.becomeCandidate(5));

  ASSERT_TRUE(state.observed(5, {}));
  ASSERT_TRUE(state.becomeCandidate(5));
  ASSERT_TRUE(state.ascend(5));

  snapshot = {5, RaftStatus::LEADER, myself, myself};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  ASSERT_TRUE(state.observed(6, nodes[0]));
  snapshot = {6, RaftStatus::FOLLOWER, nodes[0], RaftState::BLOCKED_VOTE };
  ASSERT_EQ(state.getSnapshot(), snapshot);
}

{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  RaftStateSnapshot snapshot = {6, RaftStatus::FOLLOWER, {}, RaftState::BLOCKED_VOTE };
  ASSERT_EQ(state.getSnapshot(), snapshot);

  // can't become an observer, part of the cluster
  ASSERT_FALSE(state.becomeObserver(6));

  // let's erase ourselves from the cluster and become an observer
  nodes.erase(nodes.begin()+1);
  journal.setNodes(nodes);

  ASSERT_TRUE(state.becomeObserver(6));
  snapshot = {6, RaftStatus::OBSERVER, {}, RaftState::BLOCKED_VOTE};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  ASSERT_TRUE(state.observed(6, nodes[0]));
  snapshot = {6, RaftStatus::OBSERVER, nodes[0], RaftState::BLOCKED_VOTE};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  ASSERT_TRUE(state.observed(7, {}));
  snapshot = {7, RaftStatus::OBSERVER, {}, {}};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  // cannot become candidate, I'm only an observer
  ASSERT_FALSE(state.becomeCandidate(7));
  ASSERT_FALSE(state.ascend(7));

  // try to re-enter the cluster without being part of the nodes
  ASSERT_FALSE(state.joinCluster(7));
  ASSERT_TRUE(state.observed(7, nodes[0]));

  // re-enter the cluster
  nodes.push_back(myself);
  journal.setNodes(nodes);

  ASSERT_TRUE(state.joinCluster(7));
  snapshot = {7, RaftStatus::FOLLOWER, nodes[0], RaftState::BLOCKED_VOTE};
  ASSERT_EQ(state.getSnapshot(), snapshot);

  ASSERT_FALSE(state.becomeObserver(7));

  // exit again..
  nodes.erase(nodes.begin()+2);
  journal.setNodes(nodes);
  ASSERT_TRUE(state.becomeObserver(7));
  snapshot = {7, RaftStatus::OBSERVER, nodes[0], RaftState::BLOCKED_VOTE};
  ASSERT_EQ(state.getSnapshot(), snapshot);
}
{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  RaftStateSnapshot snapshot = {7, RaftStatus::OBSERVER, {}, RaftState::BLOCKED_VOTE};
  ASSERT_EQ(state.getSnapshot(), snapshot);
  ASSERT_EQ(journal.getCurrentTerm(), 7);
  ASSERT_EQ(journal.getVotedFor(), RaftState::BLOCKED_VOTE);

  // re-enter cluster
  nodes.push_back(myself);
  journal.setNodes(nodes);
  ASSERT_TRUE(state.joinCluster(7));

  ASSERT_TRUE(state.observed(8, {}));
  ASSERT_TRUE(state.grantVote(8, nodes[1]));
}
{
  RaftJournal journal(dbpath);

  RaftState state(journal, myself);
  RaftStateSnapshot snapshot = {8, RaftStatus::FOLLOWER, {}, nodes[1]};
  ASSERT_EQ(state.getSnapshot(), snapshot);
}

}
