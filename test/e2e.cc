// ----------------------------------------------------------------------
// File: e2e.cc
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
#include "Tunnel.hh"
#include "Poller.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_REPLY(reply, val) { assert_reply(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }

class Raft_e2e : public TestCluster3Nodes {};

void assert_reply(const redisReplyPtr &reply, int integer) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_INTEGER);
  ASSERT_EQ(reply->integer, integer);
}

void assert_reply(const redisReplyPtr &reply, const std::string &str) {
  ASSERT_NE(reply, nullptr);
  // ASSERT_TRUE(reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS);
  EXPECT_EQ(std::string(reply->str, reply->len), str);
}

// crazy C++ templating to allow ASSERT_REPLY() to work as one liner in all cases
// T&& here is a universal reference
template<typename T>
void assert_reply(std::future<redisReplyPtr> &fut, T&& check) {
  assert_reply(fut.get(), check);
}

template<typename T>
void assert_reply(std::future<redisReplyPtr> &&fut, T&& check) {
  assert_reply(fut.get(), check);
}

TEST_F(Raft_e2e, coup) {
  prepare(0); prepare(1); prepare(2);
  spinup(0); spinup(1); spinup(2);

  // wait for consensus
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  int leaderID = getLeaderID();
  ASSERT_GE(leaderID, 0);
  ASSERT_LE(leaderID, 2);

  int instigator = (leaderID+1)%3;
  ASSERT_REPLY(tunnel(instigator)->exec("RAFT_COUP_DETAT"), "vive la revolution");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ASSERT_EQ(instigator, getLeaderID());
  ASSERT_TRUE(all_identical(retrieveLeaders()));
}

TEST_F(Raft_e2e, simultaneous_clients) {
  prepare(0); prepare(1); prepare(2);
  spinup(0); spinup(1); spinup(2);

  // wait for consensus..
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  int leaderID = getServerID(state(0)->getSnapshot().leader);
  ASSERT_GE(leaderID, 0);
  ASSERT_LE(leaderID, 2);

  std::vector<std::future<redisReplyPtr>> futures;

  // send off many requests, pipeline them
  futures.emplace_back(tunnel(leaderID)->execute({"get", "asdf"}));
  futures.emplace_back(tunnel(leaderID)->execute({"ping"}));
  futures.emplace_back(tunnel(leaderID)->execute({"set", "asdf", "1234"}));
  futures.emplace_back(tunnel(leaderID)->execute({"get", "asdf"}));

  ASSERT_REPLY(futures[0], "");
  ASSERT_REPLY(futures[1], "PONG");
  ASSERT_REPLY(futures[2], "OK");
  ASSERT_REPLY(futures[3], "1234");

  futures.clear();
  futures.emplace_back(tunnel(leaderID)->execute({"set", "asdf", "3456"}));
  futures.emplace_back(tunnel(leaderID)->execute({"get", "asdf"}));

  ASSERT_REPLY(futures[0], "OK");
  ASSERT_REPLY(futures[1], "3456");

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // make sure the log entry has been propagated to all nodes
  for(size_t i = 0; i < 3; i++) {
    std::string value;
    ASSERT_TRUE(rocksdb(i)->get("asdf", value).ok());
    ASSERT_EQ(value, "3456");
  }

  ASSERT_REPLY(tunnel(leaderID)->exec("set", "qwerty", "789"), "OK");
  futures.clear();

  // interwine pipelined requests from three connections
  Tunnel tunnel2(myself(leaderID).hostname, myself(leaderID).port);
  Tunnel tunnel3(myself(leaderID).hostname, myself(leaderID).port);

  futures.emplace_back(tunnel2.exec("get", "qwerty"));
  futures.emplace_back(tunnel(leaderID)->exec("set", "client2", "val"));
  futures.emplace_back(tunnel(leaderID)->exec("get", "client2"));
  futures.emplace_back(tunnel(leaderID)->exec("sadd", "myset", "a"));
  futures.emplace_back(tunnel2.exec("sadd", "myset", "b"));
  futures.emplace_back(tunnel3.exec("set", "client3", "myval"));
  futures.emplace_back(tunnel3.exec("get", "client3"));

  // not guaranteed that response will be "myval" here, since it's on a different connection
  futures.emplace_back(tunnel2.exec("get", "client3"));

  ASSERT_REPLY(futures[0], "789");
  ASSERT_REPLY(futures[1], "OK");
  ASSERT_REPLY(futures[2], "val");
  ASSERT_REPLY(futures[3], 1);
  ASSERT_REPLY(futures[4], 1);
  ASSERT_REPLY(futures[5], "OK");
  ASSERT_REPLY(futures[6], "myval");

  redisReplyPtr reply = futures[7].get();
  std::string str = std::string(reply->str, reply->len);
  qdb_info("Race-y request: GET client3 ==> " << str);
  ASSERT_TRUE(str == "myval" || str == "");

  ASSERT_REPLY(tunnel2.exec("scard", "myset"), 2);

  // but here we've received an ack - response _must_ be myval
  ASSERT_REPLY(tunnel2.exec("get", "client3"), "myval");

  RaftInfo info = dispatcher(leaderID)->info();
  ASSERT_EQ(info.blockedWrites, 0u);

  std::string err;
  std::string checkpointPath = SSTR(commonState.testdir << "/checkpoint");

  ASSERT_TRUE(dispatcher()->checkpoint(checkpointPath, err));
  ASSERT_FALSE(dispatcher()->checkpoint(checkpointPath, err)); // exists already

  // pretty expensive to open two extra databases, but necessary
  RocksDB checkpointSM(SSTR(checkpointPath << "/state-machine"));

  std::string tmp;
  ASSERT_OK(checkpointSM.get("client3", tmp));
  ASSERT_EQ(tmp, "myval");

  ASSERT_OK(checkpointSM.get("client2", tmp));
  ASSERT_EQ(tmp, "val");

  // TODO: verify checkpointSM last applied, once atomic commits are implemented

  // ensure the checkpoint journal is identical to the original
  RaftJournal checkpointJournal(SSTR(checkpointPath << "/raft-journal"));
  ASSERT_EQ(checkpointJournal.getLogSize(), journal()->getLogSize());
  for(LogIndex i = 0; i < journal()->getLogSize(); i++) {
    RaftEntry entry1, entry2;

    ASSERT_OK(checkpointJournal.fetch(i, entry1));
    ASSERT_OK(journal()->fetch(i, entry2));

    ASSERT_EQ(entry1, entry2);
  }
}

TEST_F(Raft_e2e, replication_with_trimmed_journal) {
  prepare(0); prepare(1);
  spinup(0); spinup(1);

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  int leaderID = getServerID(state(0)->getSnapshot().leader);
  int firstSlaveID = (leaderID+1)%2;
  ASSERT_GE(leaderID, 0);
  ASSERT_LE(leaderID, 1);

  std::vector<std::future<redisReplyPtr>> futures;

  // send off many requests, pipeline them
  for(size_t i = 0; i < testreqs.size(); i++) {
    futures.emplace_back(tunnel(leaderID)->execute(testreqs[i]));
  }

  for(size_t i = 0; i < 2; i++) {
    ASSERT_REPLY(futures[i], "OK");
  }

  for(size_t i = 2; i < futures.size(); i++) {
    ASSERT_REPLY(futures[i], 1);
  }

  // now let's trim leader's journal..
  journal(leaderID)->trimUntil(4);

  // and verify it's NOT possible to bring node #2 up to date
  spinup(2);

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // verify consensus on leader
  ASSERT_EQ(state(2)->getSnapshot().leader, state(0)->getSnapshot().leader);
  ASSERT_EQ(state(2)->getSnapshot().leader, state(1)->getSnapshot().leader);

  ASSERT_EQ(journal(2)->getLogSize(), 1);
  ASSERT_EQ(journal(2)->getLogStart(), 0);

  // a divine intervention fills up the missing entries in node #2 journal
  for(size_t i = 1; i < 5; i++) {
    RaftEntry entry;
    ASSERT_TRUE(journal(firstSlaveID)->fetch(i, entry).ok());

    journal(2)->append(i, entry.term, entry.request);
  }

  // now verify node #2 can be brought up to date successfully
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(journal(2)->getLogSize(), journal(leaderID)->getLogSize());
  ASSERT_EQ(journal(2)->getLogSize(), journal(firstSlaveID)->getLogSize());
}
