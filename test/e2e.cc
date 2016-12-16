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
#include "Poller.hh"
#include "Configuration.hh"
#include "QuarkDBNode.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>
#include <qclient/qclient.hh>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_REPLY(reply, val) { assert_reply(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }
#define ASSERT_ERR(reply, val) { assert_error(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }

class Raft_e2e : public TestCluster3Nodes {};
class Raft_e2e5 : public TestCluster5Nodes {};

void assert_error(const redisReplyPtr &reply, const std::string &err) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_ERROR);
  ASSERT_EQ(std::string(reply->str, reply->len), err);
}

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

void assert_reply(const redisReplyPtr &reply, const std::vector<std::string> &vec) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, vec.size());

  for(size_t i = 0; i < vec.size(); i++) {
    ASSERT_REPLY(redisReplyPtr(reply->element[i], [](redisReply*){}), vec[i]);
  }
}

void assert_reply(const redisReplyPtr &reply, const std::pair<std::string, std::vector<std::string>> &scan) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, 2u);
  ASSERT_REPLY(redisReplyPtr(reply->element[0], [](redisReply*){}), scan.first);
  ASSERT_REPLY(redisReplyPtr(reply->element[1], [](redisReply*){}), scan.second);
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
  spinup(0); spinup(1); spinup(2);

  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();
  ASSERT_GE(leaderID, 0);
  ASSERT_LE(leaderID, 2);

  int instigator = (leaderID+1)%3;
  ASSERT_REPLY(tunnel(instigator)->exec("RAFT_ATTEMPT_COUP"), "vive la revolution");
  RETRY_ASSERT_TRUE(instigator == getLeaderID());
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
}

TEST_F(Raft_e2e, simultaneous_clients) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

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

  // make sure the log entry has been propagated to all nodes
  for(size_t i = 0; i < 3; i++) {
    std::string value;
    RETRY_ASSERT_TRUE(rocksdb(i)->get("asdf", value).ok() && value == "3456");
  }

  ASSERT_REPLY(tunnel(leaderID)->exec("set", "qwerty", "789"), "OK");
  futures.clear();

  // interwine pipelined requests from three connections
  qclient::QClient tunnel2(myself(leaderID).hostname, myself(leaderID).port);
  qclient::QClient tunnel3(myself(leaderID).hostname, myself(leaderID).port);

  futures.emplace_back(tunnel2.exec("get", "qwerty"));
  futures.emplace_back(tunnel(leaderID)->exec("set", "client2", "val"));
  futures.emplace_back(tunnel(leaderID)->exec("get", "client2"));
  futures.emplace_back(tunnel(leaderID)->exec("sadd", "myset", "a"));
  futures.emplace_back(tunnel2.exec("sadd", "myset", "b"));
  futures.emplace_back(tunnel2.exec("sadd", "myset")); // malformed request
  futures.emplace_back(tunnel3.exec("set", "client3", "myval"));
  futures.emplace_back(tunnel3.exec("get", "client3"));

  // not guaranteed that response will be "myval" here, since it's on a different connection
  futures.emplace_back(tunnel2.exec("get", "client3"));

  ASSERT_REPLY(futures[0], "789");
  ASSERT_REPLY(futures[1], "OK");
  ASSERT_REPLY(futures[2], "val");
  ASSERT_REPLY(futures[3], 1);
  ASSERT_REPLY(futures[4], 1);
  ASSERT_REPLY(futures[5], "ERR wrong number of arguments for 'sadd' command");
  ASSERT_REPLY(futures[6], "OK");
  ASSERT_REPLY(futures[7], "myval");

  redisReplyPtr reply = futures[8].get();
  std::string str = std::string(reply->str, reply->len);
  qdb_info("Race-y request: GET client3 ==> " << str);
  ASSERT_TRUE(str == "myval" || str == "");

  ASSERT_REPLY(tunnel2.exec("scard", "myset"), 2);

  // but here we've received an ack - response _must_ be myval
  ASSERT_REPLY(tunnel2.exec("get", "client3"), "myval");

  RaftInfo info = dispatcher(leaderID)->info();
  ASSERT_EQ(info.blockedWrites, 0u);
  ASSERT_EQ(info.leader, myself(leaderID));

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

TEST_F(Raft_e2e, hscan) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getServerID(state(0)->getSnapshot().leader);

  for(size_t i = 1; i < 10; i++) {
    ASSERT_REPLY(tunnel(leaderID)->execute(make_req("hset", "hash", SSTR("f" << i), SSTR("v" << i))), 1);
  }

  redisReplyPtr reply = tunnel(leaderID)->execute(make_req("hscan", "hash", "0", "cOUnT", "3")).get();
  ASSERT_REPLY(reply, std::make_pair("next:f4", make_req("f1", "v1", "f2", "v2", "f3", "v3")));

  reply = tunnel(leaderID)->execute(make_req("hscan", "hash", "0", "asdf", "123")).get();
  ASSERT_ERR(reply, "ERR syntax error");

  reply = tunnel(leaderID)->execute(make_req("hscan", "hash", "next:f4", "COUNT", "3")).get();
  ASSERT_REPLY(reply, std::make_pair("next:f7", make_req("f4", "v4", "f5", "v5", "f6", "v6")));

  reply = tunnel(leaderID)->execute(make_req("hscan", "hash", "next:f7", "COUNT", "30")).get();
  ASSERT_REPLY(reply, std::make_pair("0", make_req("f7", "v7", "f8", "v8", "f9", "v9")));

  reply = tunnel(leaderID)->execute(make_req("hscan", "hash", "adfaf")).get();
  ASSERT_ERR(reply, "ERR invalid cursor");

  reply = tunnel(leaderID)->execute(make_req("hscan", "hash", "next:zz")).get();
  ASSERT_REPLY(reply, std::make_pair("0", make_req()));
}

TEST_F(Raft_e2e, test_many_redis_commands) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getServerID(state(0)->getSnapshot().leader);

  std::vector<std::future<redisReplyPtr>> futures;
  futures.emplace_back(tunnel(leaderID)->exec("SADD", "myset", "a", "b", "c"));
  futures.emplace_back(tunnel(leaderID)->exec("SCARD", "myset"));
  futures.emplace_back(tunnel(leaderID)->exec("Smembers", "myset"));
  futures.emplace_back(tunnel(leaderID)->exec("srem", "myset", "a", "b"));
  futures.emplace_back(tunnel(leaderID)->exec("srem", "myset", "b"));
  futures.emplace_back(tunnel(leaderID)->exec("scard", "myset"));
  futures.emplace_back(tunnel(leaderID)->exec("smembers", "myset"));

  ASSERT_REPLY(futures[0], 3);
  ASSERT_REPLY(futures[1], 3);
  ASSERT_REPLY(futures[2], make_req("a", "b", "c"));
  ASSERT_REPLY(futures[3], 2);
  ASSERT_REPLY(futures[4], 0);
  ASSERT_REPLY(futures[5], 1);
  ASSERT_REPLY(futures[6], make_req("c"));

  futures.clear();

  futures.emplace_back(tunnel(leaderID)->exec("hset", "myhash", "a", "b"));
  futures.emplace_back(tunnel(leaderID)->exec("hset", "myhash", "b", "c"));
  futures.emplace_back(tunnel(leaderID)->exec("hset", "myhash", "c", "d"));
  futures.emplace_back(tunnel(leaderID)->exec("hset", "myhash", "a", "d"));
  futures.emplace_back(tunnel(leaderID)->exec("hdel", "myhash", "a", "b", "b"));
  futures.emplace_back(tunnel(leaderID)->exec("hdel", "myhash", "a"));

  ASSERT_REPLY(futures[0], 1);
  ASSERT_REPLY(futures[1], 1);
  ASSERT_REPLY(futures[2], 1);
  ASSERT_REPLY(futures[3], 0);
  ASSERT_REPLY(futures[4], 2);
  ASSERT_REPLY(futures[5], 0);

  futures.clear();
  futures.emplace_back(tunnel(leaderID)->exec("set", "mystring", "asdf"));
  futures.emplace_back(tunnel(leaderID)->exec("keys", "*"));
  futures.emplace_back(tunnel(leaderID)->exec("exists", "mystring", "myset", "myhash", "adfa", "myhash"));
  futures.emplace_back(tunnel(leaderID)->exec("del", "myhash", "myset", "mystring"));
  futures.emplace_back(tunnel(leaderID)->exec("exists", "mystring", "myset", "myhash", "adfa", "myhash"));
  futures.emplace_back(tunnel(leaderID)->exec("del", "myhash", "myset"));

  ASSERT_REPLY(futures[0], "OK");
  ASSERT_REPLY(futures[1], make_req("mystring", "myhash", "myset"));
  ASSERT_REPLY(futures[2], 4);
  ASSERT_REPLY(futures[3], 3);
  ASSERT_REPLY(futures[4], 0);
  ASSERT_REPLY(futures[5], 0);

  futures.clear();
  futures.emplace_back(tunnel(leaderID)->exec("set", "a", "aa"));
  futures.emplace_back(tunnel(leaderID)->exec("set", "aa", "a"));
  futures.emplace_back(tunnel(leaderID)->exec("get", "a"));
  futures.emplace_back(tunnel(leaderID)->exec("del", "a"));
  futures.emplace_back(tunnel(leaderID)->exec("get", "aa"));
  futures.emplace_back(tunnel(leaderID)->exec("keys", "*"));

  ASSERT_REPLY(futures[0], "OK");
  ASSERT_REPLY(futures[1], "OK");
  ASSERT_REPLY(futures[2], "aa");
  ASSERT_REPLY(futures[3], 1);
  ASSERT_REPLY(futures[4], "a");
  ASSERT_REPLY(futures[5], make_req("aa"));

  futures.clear();
  futures.emplace_back(tunnel(leaderID)->exec("flushall"));
  futures.emplace_back(tunnel(leaderID)->exec("del", "aa"));

  ASSERT_REPLY(futures[0], "OK");
  ASSERT_REPLY(futures[1], 0);

  futures.clear();
  futures.emplace_back(tunnel(leaderID)->exec("hset", "hash", "key1", "v1"));
  futures.emplace_back(tunnel(leaderID)->exec("hset", "hash2", "key1", "v1"));
  futures.emplace_back(tunnel(leaderID)->exec("exists", "hash", "hash2"));
  futures.emplace_back(tunnel(leaderID)->exec("del", "hash"));
  futures.emplace_back(tunnel(leaderID)->exec("exists", "hash"));
  futures.emplace_back(tunnel(leaderID)->exec("exists", "hash2"));

  ASSERT_REPLY(futures[0], 1);
  ASSERT_REPLY(futures[1], 1);
  ASSERT_REPLY(futures[2], 2);
  ASSERT_REPLY(futures[3], 1);
  ASSERT_REPLY(futures[4], 0);
  ASSERT_REPLY(futures[5], 1);
}

TEST_F(Raft_e2e, replication_with_trimmed_journal) {
  spinup(0); spinup(1);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1));

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
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  ASSERT_EQ(journal(2)->getLogSize(), 1);
  ASSERT_EQ(journal(2)->getLogStart(), 0);

  // a divine intervention fills up the missing entries in node #2 journal
  for(size_t i = 1; i < 5; i++) {
    RaftEntry entry;
    ASSERT_TRUE(journal(firstSlaveID)->fetch(i, entry).ok());

    journal(2)->append(i, entry.term, entry.request);
  }

  // now verify node #2 can be brought up to date successfully
  RETRY_ASSERT_TRUE(
    journal(0)->getLogSize() == journal(1)->getLogSize() &&
    journal(1)->getLogSize() == journal(2)->getLogSize()
  );

  ASSERT_EQ(journal(2)->getLogSize(), journal(leaderID)->getLogSize());
  ASSERT_EQ(journal(2)->getLogSize(), journal(firstSlaveID)->getLogSize());
}

TEST_F(Raft_e2e, membership_updates) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getServerID(state(0)->getSnapshot().leader);
  ASSERT_REPLY(tunnel(leaderID)->exec("set", "pi", "3.141516"), "OK");

  // throw a node out of the cluster
  int victim = (leaderID+1) % 3;
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_REMOVE_MEMBER", myself(victim).toString()), "OK");
  RETRY_ASSERT_TRUE(dispatcher(leaderID)->info().commitIndex == 2);

  // verify the cluster has not been disrupted
  ASSERT_EQ(state(leaderID)->getSnapshot().leader, myself(leaderID));

  // add it back as an observer, verify consensus
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_ADD_OBSERVER", myself(victim).toString()), "OK");

  RETRY_ASSERT_TRUE(dispatcher(0)->info().commitIndex == 3);
  RETRY_ASSERT_TRUE(dispatcher(1)->info().commitIndex == 3);
  RETRY_ASSERT_TRUE(dispatcher(2)->info().commitIndex == 3);

  ASSERT_EQ(state(victim)->getSnapshot().status, RaftStatus::FOLLOWER);

  ASSERT_EQ(state(0)->getSnapshot().leader, state(1)->getSnapshot().leader);
  ASSERT_EQ(state(1)->getSnapshot().leader, state(2)->getSnapshot().leader);

  ASSERT_EQ(journal(0)->getLogSize(), journal(1)->getLogSize());
  ASSERT_EQ(journal(1)->getLogSize(), journal(2)->getLogSize());

  // cannot be a leader, it's an observer
  ASSERT_NE(state(0)->getSnapshot().leader, myself(victim));

  // add back as a full voting member
  leaderID = getServerID(state(0)->getSnapshot().leader);
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_PROMOTE_OBSERVER", myself(victim).toString()), "OK");
  RETRY_ASSERT_TRUE(dispatcher(leaderID)->info().commitIndex == 4);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
}

TEST_F(Raft_e2e5, membership_updates_with_disruptions) {
  // let's get this party started
  spinup(0); spinup(1); spinup(2); spinup(3);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2, 3));

  // verify consensus
  for(size_t i = 1; i < 3; i++) {
    ASSERT_EQ(state(i)->getSnapshot().leader, state(i-1)->getSnapshot().leader);
  }

  // throw node #4 out of the cluster
  int leaderID = getServerID(state(0)->getSnapshot().leader);
  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_REMOVE_MEMBER", myself(4).toString()), "OK");
  RETRY_ASSERT_TRUE(dispatcher(leaderID)->info().commitIndex == 1);

  // .. and now spinup node #4 :> Ensure it doesn't disrupt the current leader
  spinup(4);
  std::this_thread::sleep_for(raftclock()->getTimeouts().getHigh()*2);
  ASSERT_EQ(leaderID, getServerID(state(0)->getSnapshot().leader));

  // verify the cluster has not been disrupted
  ASSERT_EQ(state(leaderID)->getSnapshot().leader, myself(leaderID));

  // remove one more node
  int victim = (leaderID+1) % 5;
  if(victim == 4) victim = 2;

  ASSERT_REPLY(tunnel(leaderID)->exec("RAFT_REMOVE_MEMBER", myself(victim).toString()), "OK");
  std::this_thread::sleep_for(raftclock()->getTimeouts().getHigh()*2);

  // verify the cluster has not been disrupted
  ASSERT_EQ(state(leaderID)->getSnapshot().leader, myself(leaderID));

  // issue a bunch of writes and reads
  ASSERT_REPLY(tunnel(leaderID)->exec("set", "123", "abc"), "OK");
  ASSERT_REPLY(tunnel(leaderID)->exec("get", "123"), "abc");
}
