// ----------------------------------------------------------------------
// File: multi.cc
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

#include "redis/Transaction.hh"
#include "raft/RaftContactDetails.hh"
#include "test-utils.hh"
#include "test-reply-macros.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>

using namespace quarkdb;
using namespace qclient;

class Multi : public TestCluster3NodesFixture {};

TEST_F(Multi, Dispatching) {
  RedisDispatcher dispatcher(*stateMachine(), *publisher());

  Transaction tx1;
  tx1.emplace_back("GET", "aaa");
  tx1.emplace_back("SET", "aaa", "bbb");
  tx1.emplace_back("GET", "aaa");
  tx1.setPhantom(false);

  RedisEncodedResponse resp = dispatcher.dispatch(tx1, 1);
  ASSERT_EQ(resp.val, "*3\r\n$-1\r\n+OK\r\n$3\r\nbbb\r\n");

  RedisRequest req = {"GET", "aaa"};
  resp = dispatcher.dispatch(req, 0);
  ASSERT_EQ(resp.val, "$3\r\nbbb\r\n");
}

TEST_F(Multi, HandlerBasicSanity) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();

  std::vector<std::future<redisReplyPtr>> replies;
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("SET", "key", "value"));
  replies.push_back(tunnel(leaderID)->exec("SET", "key-2", "val-2"));

  ASSERT_REPLY(replies[0], "OK");
  ASSERT_REPLY(replies[1], "QUEUED");
  ASSERT_REPLY(replies[2], "QUEUED");

  // No dirty reads
  QClient connection2(myself(leaderID).hostname, myself(leaderID).port, makeNoRedirectOptions());
  ASSERT_REPLY(connection2.exec("GET", "key"), "");

  redisReplyPtr reply = tunnel(leaderID)->exec("EXEC").get();
  ASSERT_EQ(qclient::describeRedisReply(reply), "1) OK\n2) OK\n");

  ASSERT_REPLY(connection2.exec("GET", "key"), "value");

  // Empty multi/exec block
  replies.clear();
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("EXEC"));

  ASSERT_REPLY(replies[0], "OK");
  reply = replies[1].get();

  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, 0u);

  // No double MULTI
  replies.clear();
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("MULTI", "aaaa"));
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("SET", "counter", "1"));
  replies.push_back(tunnel(leaderID)->exec("HSET", "myhash", "f1", "v1"));
  replies.push_back(tunnel(leaderID)->exec("EXEC"));

  ASSERT_REPLY(replies[0], "OK");
  ASSERT_REPLY(replies[1], "ERR wrong number of arguments for 'MULTI' command");
  ASSERT_REPLY(replies[2], "ERR MULTI calls can not be nested");
  ASSERT_REPLY(replies[3], "QUEUED");
  ASSERT_REPLY(replies[4], "QUEUED");

  reply = replies[5].get();
  ASSERT_EQ(qclient::describeRedisReply(reply), "1) OK\n2) (integer) 1\n");

  // Discard without multi
  ASSERT_REPLY(tunnel(leaderID)->exec("DISCARD"), "ERR DISCARD without MULTI");

  // Discard
  replies.clear();
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("HSET", "myhash", "f1", "v2"));
  replies.push_back(tunnel(leaderID)->exec("DISCARD"));
  replies.push_back(tunnel(leaderID)->exec("HGET", "myhash", "f1"));

  ASSERT_REPLY(replies[0], "OK");
  ASSERT_REPLY(replies[1], "QUEUED");
  ASSERT_REPLY(replies[2], "OK");
  ASSERT_REPLY(replies[3], "v1");

  // EXEC without MULTI
  replies.clear();
  ASSERT_REPLY(tunnel(leaderID)->exec("EXEC"), "ERR EXEC without MULTI");
  ASSERT_REPLY(tunnel(leaderID)->exec("HGET", "myhash", "f1"), "v1");

  // Read MULTI-EXEC right after a write
  replies.clear();
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("SET", "abc", "123"));
  replies.push_back(tunnel(leaderID)->exec("EXEC"));

  ASSERT_REPLY(replies[0], "OK");
  ASSERT_REPLY(replies[1], "QUEUED");
  ASSERT_REPLY_DESCRIBE(replies[2], "1) OK\n");

  replies.clear();
  replies.push_back(tunnel(leaderID)->exec("MULTI"));
  replies.push_back(tunnel(leaderID)->exec("GET", "abc"));
  replies.push_back(tunnel(leaderID)->exec("EXEC"));

  ASSERT_REPLY(replies[0], "OK");
  ASSERT_REPLY(replies[1], "QUEUED");
  ASSERT_REPLY_DESCRIBE(replies[2], "1) \"123\"\n");
}

TEST_F(Multi, WithRaft) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  Transaction write;
  write.emplace_back("SET", "aaa", "bbb");
  write.emplace_back("SET", "bbb", "ccc");
  ASSERT_EQ(write.getFusedCommand(), "TX_READWRITE");

  int leaderID = getLeaderID();

  redisReplyPtr reply = tunnel(leaderID)->exec(
    write.getFusedCommand(),
    write.serialize(),
    "real"
  ).get();

  ASSERT_EQ(qclient::describeRedisReply(reply),
    "1) OK\n"
    "2) OK\n"
  );

  write.clear();
  write.emplace_back("SET", "bbb", "ddd");
  write.emplace_back("GET", "aaa");
  ASSERT_EQ(write.getFusedCommand(), "TX_READWRITE");

  reply = tunnel(leaderID)->exec(
    write.getFusedCommand(),
    write.serialize(),
    "real"
  ).get();

  ASSERT_EQ(qclient::describeRedisReply(reply),
    "1) OK\n"
    "2) \"bbb\"\n"
  );

  Transaction read;
  read.emplace_back("GET", "aaa");
  read.emplace_back("GET", "bbb");
  ASSERT_FALSE(read.containsWrites());
  ASSERT_EQ(read.getFusedCommand(), "TX_READONLY");

  reply = tunnel(leaderID)->exec(
    read.getFusedCommand(),
    read.serialize(),
    "real"
  ).get();

  ASSERT_EQ(qclient::describeRedisReply(reply),
    "1) \"bbb\"\n"
    "2) \"ddd\"\n"
  );
}
