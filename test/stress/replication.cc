// ----------------------------------------------------------------------
// File: replication.cc
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

#include "raft/RaftDispatcher.hh"
#include "raft/RaftReplicator.hh"
#include "raft/RaftTalker.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftCommitTracker.hh"
#include "Poller.hh"
#include "Configuration.hh"
#include "QuarkDBNode.hh"
#include "../test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>
#include "utils/AssistedThread.hh"
#include "../test-reply-macros.hh"

using namespace quarkdb;
class Replication : public TestCluster3NodesFixture {};

TEST_F(Replication, entries_50k_with_follower_loss) {
  // let's get this party started
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  // push lots of updates
  const int64_t NENTRIES = 50000;
  for(size_t i = 0; i < NENTRIES; i++) {
    tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
  }

  int victimFollower = (leaderID+1)%3;
  int activeFollower = (leaderID+2)%3;

  // verify the leader has started replicating some of the entries already
  RETRY_ASSERT_TRUE(journal(victimFollower)->getCommitIndex() > 5000);

  // bring down one of the followers, ensure replication is not complete
  spindown(victimFollower);
  ASSERT_TRUE(journal(victimFollower)->getLogSize() < NENTRIES);

  // ensure that eventually, the other follower gets all entries
  RETRY_ASSERT_TRUE(journal(activeFollower)->getLogSize() >= NENTRIES+2);
  ASSERT_TRUE(journal(leaderID)->getLogSize() >= NENTRIES+2);

  RETRY_ASSERT_TRUE(stateMachine(activeFollower)->getLastApplied() >= NENTRIES+1);
  RETRY_ASSERT_TRUE(stateMachine(leaderID)->getLastApplied() >= NENTRIES+1);
}

TEST_F(Replication, lease_expires_under_load) {
  // only nodes #0 and #1 are active
  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));
  int leaderID = getLeaderID();
  int followerID = (getLeaderID()+1) % 2;

  // push lots of updates
  const int64_t NENTRIES = 50000;
  for(size_t i = 0; i < NENTRIES; i++) {
    tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
  }

  // verify the leader has started replicating some of the entries already
  RETRY_ASSERT_TRUE(journal(followerID)->getCommitIndex() > 5000);

  // bring down one the follower, ensure replication is not complete
  spindown(followerID);
  ASSERT_TRUE(journal(followerID)->getLogSize() < NENTRIES);

  // ensure the connection doesn't hang
  tunnel(leaderID)->exec("ping").get();
}

TEST_F(Replication, node_has_committed_entries_no_one_else_has_ensure_it_vetoes) {
  // node #0 has committed entries that no other node has. The node should
  // veto any attempts of election, so that only itself can win this election.

  ASSERT_TRUE(state(0)->observed(5, {}));
  ASSERT_TRUE(state(1)->observed(5, {}));
  ASSERT_TRUE(state(2)->observed(5, {}));

  // add a few requests to the log
  ASSERT_TRUE(journal()->append(1, 3, testreqs[0]));
  ASSERT_TRUE(journal()->append(2, 4, testreqs[1]));
  ASSERT_TRUE(journal()->append(3, 5, testreqs[2]));

  // commit all of them
  ASSERT_TRUE(journal()->setCommitIndex(3));

  // Here, timeouts are really important, as the veto message must go through
  // in time. Prepare the DBs before spinning up.
  prepare(0); prepare(1); prepare(2);

  // node #0 must win
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  ASSERT_EQ(state(0)->getSnapshot().status, RaftStatus::LEADER);
}

TEST_F(Replication, connection_shuts_down_before_all_replies_arrive) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  const int64_t NENTRIES = 10000;
  for(size_t repeat = 0; repeat < 3; repeat++) {
    // push lots of updates
    for(size_t i = 0; i < NENTRIES; i++) {
      tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
    }

    killTunnel(leaderID);
  }

  for(size_t i = 0; i < NENTRIES; i++) {
    tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i));
  }
  tunnel(leaderID)->exec("ping").get();
  ASSERT_TRUE(leaderID == getLeaderID());
  ASSERT_TRUE(journal()->getCommitIndex() > NENTRIES);
  // if we haven't crashed or gotten hung by now, we're grand
}

// blindly generate load, ignore any errors
static void generateLoad(qclient::QClient *qcl, std::string prefix, ThreadAssistant &assistant) {
  int counter = 0;
  while(!assistant.terminationRequested()) {
    qcl->exec("set", SSTR(prefix <<  "-key-" << counter), SSTR(prefix << "value-" << counter));
    counter++;
  }
  qdb_info("Stopping load generation towards '" << prefix << "', waiting on pending replies");
  qcl->exec("ping").get();
  qdb_info("Shutting down load generator towards '" << prefix << "'");
}

TEST_F(Replication, load_during_election) {
  // let's be extra evil and start generating load even before the nodes start up
  AssistedThread t1(generateLoad, tunnel(0), "node0");
  AssistedThread t2(generateLoad, tunnel(1), "node1");
  AssistedThread t3(generateLoad, tunnel(2), "node2");

  // start the cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  // terminate once we reach a decent number of writes
  int leaderID = getLeaderID();
  RETRY_ASSERT_TRUE(journal(leaderID)->getCommitIndex() > 20000);
  ASSERT_TRUE(leaderID == getLeaderID());

  t1.stop();
  t2.stop();
  t3.stop();
}

static void assert_linearizability(std::future<redisReplyPtr> &future, std::string expectedValue, std::atomic<int64_t> &responses, std::atomic<int64_t> &violations) {
  redisReplyPtr reply = future.get();
  if(reply->type != REDIS_REPLY_STRING) return;

  responses++;
  std::string receivedValue = std::string(reply->str, reply->len);
  if(expectedValue != receivedValue) {
    violations++;
    qdb_critical("Linearizability violation. Received " << quotes(receivedValue) << ", expected: " << quotes(expectedValue));
  }
}

// Given an endpoint, try to read a key again and again and again.
// If we get ERR or MOVED, no problem.
// If we get a response other than expectedValue, linearizability has been violated.
static void obsessiveReader(qclient::QClient *qcl, std::string key, std::string expectedValue, std::atomic<int64_t> &responses, std::atomic<int64_t> &violations, ThreadAssistant &assistant) {
  std::list<std::future<redisReplyPtr>> futures;

  qdb_info("Issuing a flood of reads for key " << quotes(key));

  while(!assistant.terminationRequested()) {
    futures.emplace_back(qcl->exec("get", key));

    while(futures.size() >= 1000) {
      assert_linearizability(futures.front(), expectedValue, responses, violations);
      futures.pop_front();
    }
  }

  while(futures.size() != 0) {
    assert_linearizability(futures.front(), expectedValue, responses, violations);
    futures.pop_front();
  }
}

TEST_F(Replication, linearizability_during_failover) {
  // start the cluster
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  std::vector<std::future<redisReplyPtr>> futures;

  // Issue a bunch of writes, all towards the same key
  const int nWrites = 10000;
  for(size_t i = 0; i <= nWrites; i++) {
    futures.push_back(tunnel(leaderID)->exec("set", "key", SSTR("value-" << i)));
  }

  // Receive responses
  for(size_t i = 0; i < futures.size(); i++) {
    ASSERT_REPLY(futures[i], "OK");
  }

  // our followers..
  int node1 = (leaderID + 1) % 3;
  int node2 = (leaderID + 2) % 3;

  // start reading "key"
  std::atomic<int64_t> responses(0), violations(0);

  AssistedThread reader1(obsessiveReader, tunnel(node1), "key", SSTR("value-" << nWrites), std::ref(responses), std::ref(violations));
  AssistedThread reader2(obsessiveReader, tunnel(node2), "key", SSTR("value-" << nWrites), std::ref(responses), std::ref(violations));

  RaftTerm firstTerm = state(leaderID)->getCurrentTerm();

  // stop the leader
  spindown(leaderID);

  // Ensure failover happens..
  RETRY_ASSERT_TRUE(state(node1)->getCurrentTerm() != firstTerm);
  RETRY_ASSERT_TRUE(checkStateConsensus(node1, node2));
  int newLeaderID = getLeaderID();
  ASSERT_NE(leaderID, newLeaderID);

  // Wait until we have 1k real responses (not errors or "moved")
  RETRY_ASSERT_TRUE(responses >= 1000);

  reader1.stop(); reader2.stop();
  reader1.join(); reader2.join();

  qdb_info("After " << responses << " reads, linearizability was violated " << violations << " times.");
  ASSERT_EQ(violations, 0);
}
