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

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_REPLY(reply, val) { assert_reply(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }
#define ASSERT_ERR(reply, val) { assert_error(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }

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
static void generateLoad(qclient::QClient *qcl, std::string prefix, std::atomic<bool> &stopFlag) {
  int counter = 0;
  while(!stopFlag) {
    qcl->exec("set", SSTR(prefix <<  "-key-" << counter), SSTR(prefix << "value-" << counter));
    counter++;
  }
  qcl->exec("ping").get();
  qdb_info("Stopping load generation towards '" << prefix << "'");
}

class AssistedThread {
public:
  // universal references, perfect forwarding
  template<typename... Args>
  AssistedThread(Args&&... args) : stopFlag(false), th(std::forward<Args>(args)..., std::ref(stopFlag)) {
  }

  virtual ~AssistedThread() {
    stopFlag = true;
    th.join();
  }
private:
  std::atomic<bool> stopFlag;
  std::thread th;
};

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
}
