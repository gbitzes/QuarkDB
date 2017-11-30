// ----------------------------------------------------------------------
// File: qclient.cc
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
#include "raft/RaftReplicator.hh"
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
class QClientTests : public TestCluster3NodesFixture {};

TEST_F(QClientTests, hide_transient_failures) {
  qclient::Members members;
  members.push_back(myself(0).hostname, myself(0).port);
  members.push_back(myself(1).hostname, myself(1).port);
  members.push_back(myself(2).hostname, myself(2).port);

  qclient::RetryStrategy retryStrategy = {true, std::chrono::seconds(30)};
  QClient qcl(members, true, retryStrategy);

  // Issue request _before_ spinning up the cluster! Verify it succeeds.
  std::future<redisReplyPtr> reply = qcl.exec("HSET", "aaaaa", "bbbbb", "cccc");

  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  ASSERT_REPLY(reply, 1);
  ASSERT_REPLY(qcl.exec("HGET", "aaaaa", "bbbbb"), "cccc");

  int leaderID = getLeaderID();
  spindown(leaderID);

  ASSERT_REPLY(qcl.exec("HSET", "aaaaa", "bbbbb", "ddd"), 0);
  ASSERT_REPLY(qcl.exec("HGET", "aaaaa", "bbbbb"), "ddd");

  spinup(leaderID);

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < 10000; i++) {
    replies.emplace_back(qcl.exec("SET", SSTR("key-" << i), SSTR("val-" << i)));

    if(i % 1024 == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      // huehueue
      int leaderID = getLeaderID();
      if(leaderID >= 0) {
        spindown(leaderID);
        spinup(leaderID);
      }
    }
  }

  for(size_t i = 0; i < 10000; i++) {
    ASSERT_REPLY(replies[i], "OK");
  }

  for(size_t i = 0; i < 10000; i++) {
    ASSERT_REPLY(qcl.exec("GET", SSTR("key-" << i)), SSTR("val-" << i));
  }
}

TEST_F(QClientTests, nullptr_only_after_timeout) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  qclient::Members members;
  members.push_back(myself(0).hostname, myself(0).port);
  members.push_back(myself(1).hostname, myself(1).port);
  members.push_back(myself(2).hostname, myself(2).port);

  qclient::RetryStrategy retryStrategy = {true, std::chrono::seconds(3)};
  QClient qcl(members, true, retryStrategy);

  ASSERT_REPLY(qcl.exec("HSET", "aaaaa", "bbbbb", "cccc"), 1);
  ASSERT_REPLY(qcl.exec("HGET", "aaaaa", "bbbbb"), "cccc");

  // kill cluster
  spindown(0); spindown(1); spindown(2);

  // ensure qclient responses don't hang
  std::future<redisReplyPtr> reply = qcl.exec("HGET", "aaaaa", "bbbbb");
  ASSERT_EQ(reply.get(), nullptr);
}
