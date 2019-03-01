// ----------------------------------------------------------------------
// File: bulkload.cc
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
#include "raft/RaftTimeouts.hh"
#include "raft/RaftCommitTracker.hh"
#include "raft/RaftReplicator.hh"
#include "ShardDirectory.hh"
#include "Poller.hh"
#include "Configuration.hh"
#include "QuarkDBNode.hh"
#include "../test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>
#include "utils/AssistedThread.hh"
#include "../test-reply-macros.hh"

#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
using namespace quarkdb;

TEST(BulkLoad, BasicSanity) {
  system("rm -rf /tmp/quarkdb-bulkload-test");

  {
  StateMachine stateMachine("/tmp/quarkdb-bulkload-test", false, true);

  for(size_t i = 0; i < 100; i++) {
    bool created;
    ASSERT_OK(stateMachine.hset("some-key", SSTR("field-" << i), "value", created));
    ASSERT_TRUE(created);

    ASSERT_OK(stateMachine.hset(SSTR("some-key-" << i), "field", "value", created));
    ASSERT_TRUE(created);

    ASSERT_OK(stateMachine.hset(SSTR("some-key-" << i), "field", "value", created));
    ASSERT_TRUE(created);

    ASSERT_OK(stateMachine.set(SSTR("a-" << i), SSTR("v-" << i)));
    ASSERT_OK(stateMachine.set(SSTR("z#|#-" << i), SSTR("vz-" << i)));

    RedisRequest items;
    items.push_back(SSTR(i));
    items.push_back(SSTR(i+1));
    items.push_back(SSTR(i+200));

    int64_t ignored;
    ASSERT_OK(stateMachine.sadd(SSTR("some-set-" << i), items.begin(), items.end(), ignored));
    ASSERT_OK(stateMachine.sadd("some-set", items.begin(), items.end(), ignored));

    ASSERT_OK(stateMachine.lhset("locality-hash-1", SSTR("field-" << i), SSTR("hint-" << i), SSTR("lh1-value-" << i), created));
    ASSERT_TRUE(created);

    ASSERT_OK(stateMachine.lhset("locality-hash-2", "field", "hint", SSTR("lh2-value-" << i), created));
    ASSERT_TRUE(created);

    ASSERT_OK(stateMachine.lhset("locality-hash-3", SSTR("field-" << i), "hint", SSTR("lh3-value-" << i), created));
    ASSERT_TRUE(created);
  }


  stateMachine.finalizeBulkload();
  }

  size_t len;
  StateMachine stateMachine("/tmp/quarkdb-bulkload-test");
  ASSERT_OK(stateMachine.hlen("some-key", len));
  ASSERT_EQ(len, 100u);

  ASSERT_OK(stateMachine.scard("some-set", len));
  ASSERT_EQ(len, 201u);

  ASSERT_OK(stateMachine.lhlen("locality-hash-1", len));
  ASSERT_EQ(len, 100u);

  ASSERT_OK(stateMachine.lhlen("locality-hash-2", len));
  ASSERT_EQ(len, 1u);

  ASSERT_OK(stateMachine.lhlen("locality-hash-3", len));
  ASSERT_EQ(len, 100u);

  std::string contents;
  ASSERT_OK(stateMachine.lhget("locality-hash-2", "field", "", contents));
  ASSERT_EQ(contents, "lh2-value-99");

  ASSERT_OK(stateMachine.lhget("locality-hash-2", "field", "wrong-hint", contents));
  ASSERT_EQ(contents, "lh2-value-99");

  ASSERT_OK(stateMachine.lhget("locality-hash-2", "field", "hint", contents));
  ASSERT_EQ(contents, "lh2-value-99");

  for(size_t i = 0; i < 100; i++) {
    ASSERT_OK(stateMachine.hlen(SSTR("some-key-" << i), len));
    ASSERT_EQ(len, 1u);

    std::string v;
    ASSERT_OK(stateMachine.get(SSTR("a-" << i), v));
    ASSERT_EQ(v, SSTR("v-" << i));

    ASSERT_OK(stateMachine.get(SSTR("z#|#-" << i), v));
    ASSERT_EQ(v, SSTR("vz-" << i));

    ASSERT_OK(stateMachine.scard(SSTR("some-set-" << i), len));
    ASSERT_EQ(len, 3u);

    ASSERT_OK(stateMachine.lhget("locality-hash-1", SSTR("field-" << i), "", contents));
    ASSERT_EQ(contents, SSTR("lh1-value-" << i));

    ASSERT_OK(stateMachine.lhget("locality-hash-1", SSTR("field-" << i), "wrong-hint", contents));
    ASSERT_EQ(contents, SSTR("lh1-value-" << i));

    ASSERT_OK(stateMachine.lhget("locality-hash-1", SSTR("field-" << i), SSTR("hint-" << i), contents));
    ASSERT_EQ(contents, SSTR("lh1-value-" << i));

    ASSERT_OK(stateMachine.lhget("locality-hash-3", SSTR("field-" << i), "", contents));
    ASSERT_EQ(contents, SSTR("lh3-value-" << i));

    ASSERT_OK(stateMachine.lhget("locality-hash-3", SSTR("field-" << i), "wrong-hint", contents));
    ASSERT_EQ(contents, SSTR("lh3-value-" << i));

    ASSERT_OK(stateMachine.lhget("locality-hash-3", SSTR("field-" << i), "hint", contents));
    ASSERT_EQ(contents, SSTR("lh3-value-" << i));
  }
}

TEST(BulkLoad, PanicWhenOpeningUnfinalizedStateMachine) {
  ASSERT_EQ(system("rm -rf /tmp/quarkdb-bulkload-test"), 0);

  {
  StateMachine stateMachine("/tmp/quarkdb-bulkload-test", false, true);

  for(size_t i = 0; i < 100; i++) {
    bool created;
    ASSERT_OK(stateMachine.hset("some-key", SSTR("field-" << i), "value", created));
    ASSERT_TRUE(created);
  }

  }

  ASSERT_THROW(StateMachine("/tmp/quarkdb-bulkload-test"), FatalException);
}

TEST(Bulkload, RaftJournalAtNonZeroIndex) {
  ASSERT_EQ(system("rm -rf /tmp/quarkdb-tests-raft-journal"), 0);

  {
    RaftServer srv {"localhost", 2222};
    RaftJournal journal("/tmp/quarkdb-tests-raft-journal", "some-uuid", {srv}, 1337);

    ASSERT_EQ(journal.getLogSize(), 1338);
    ASSERT_EQ(journal.getLogStart(), 1337);
    ASSERT_EQ(journal.getCommitIndex(), 1337);
    ASSERT_EQ(journal.getEpoch(), 1337);

    RaftEntry entry;
    ASSERT_TRUE(journal.fetch(1337, entry).ok());
    ASSERT_EQ(entry, RaftEntry(0, "JOURNAL_UPDATE_MEMBERS", "localhost:2222|", "some-uuid"));
  }
}

TEST(Bulkload, CreateConsensusShardFromExistingSM) {
  ASSERT_EQ(system("rm -rf /tmp/quarkdb-tests-shard-from-existing-sm"), 0);
  ASSERT_EQ(system("mkdir /tmp/quarkdb-tests-shard-from-existing-sm"), 0);

  std::unique_ptr<StateMachine> sm;
  sm.reset(new StateMachine("/tmp/quarkdb-tests-shard-from-existing-sm/original-sm"));

  ASSERT_EQ(sm->getLastApplied(), 0);
  ASSERT_EQ(sm->getPhysicalLocation(), "/tmp/quarkdb-tests-shard-from-existing-sm/original-sm");
  ASSERT_TRUE(sm->set("my-key", "123", 1).ok());

  std::string value;
  ASSERT_TRUE(sm->get("my-key", value).ok());
  ASSERT_EQ(value, "123");

  ASSERT_EQ(sm->getLastApplied(), 1);

  std::unique_ptr<ShardDirectory> shardDir;
  RaftServer srv("localhost", 123);
  shardDir.reset(ShardDirectory::create("/tmp/quarkdb-tests-shard-from-existing-sm/shard", "cluster-id", "shar-id", {srv}, 99, std::move(sm) ));
  ASSERT_EQ(sm.get(), nullptr);

  ASSERT_EQ(shardDir->getStateMachine()->getPhysicalLocation(), "/tmp/quarkdb-tests-shard-from-existing-sm/shard/current/state-machine");
  ASSERT_EQ(shardDir->getStateMachine()->getLastApplied(), 99);

  ASSERT_TRUE(shardDir->getStateMachine()->get("my-key", value).ok());
}
