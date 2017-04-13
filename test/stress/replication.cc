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
  RETRY_ASSERT_TRUE_10sec(journal(victimFollower)->getCommitIndex() > 5000);

  // bring down one of the followers, ensure replication is not complete
  spindown(victimFollower);
  ASSERT_TRUE(journal(victimFollower)->getLogSize() < NENTRIES);

  // ensure that eventually, the other follower gets all entries
  RETRY_ASSERT_TRUE_10sec(journal(activeFollower)->getLogSize() >= NENTRIES+2);
  ASSERT_TRUE(journal(leaderID)->getLogSize() >= NENTRIES+2);

  RETRY_ASSERT_TRUE_10sec(stateMachine(activeFollower)->getLastApplied() >= NENTRIES+1);
  RETRY_ASSERT_TRUE_10sec(stateMachine(leaderID)->getLastApplied() >= NENTRIES+1);
}
