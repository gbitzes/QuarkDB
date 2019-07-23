// ----------------------------------------------------------------------
// File: recovery.cc
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

#include "test-reply-macros.hh"
#include "raft/RaftJournal.hh"
#include "recovery/RecoveryEditor.hh"
#include "recovery/RecoveryRunner.hh"
#include "storage/KeyLocators.hh"
#include "StateMachine.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_NOTFOUND(msg) ASSERT_TRUE(msg.IsNotFound())

TEST(Recovery, BasicSanity) {
  {
    ASSERT_EQ(system("rm -rf /tmp/quarkdb-recovery-test"), 0);
    StateMachine sm("/tmp/quarkdb-recovery-test");
    ASSERT_OK(sm.set("abc", "123", 1));
    ASSERT_OK(sm.set("abc", "234", 2));
  }

  {
    RecoveryEditor recovery("/tmp/quarkdb-recovery-test");

    std::string val;
    ASSERT_OK(recovery.get(DescriptorLocator("abc").toString(), val));
    KeyDescriptor descr(val);
    ASSERT_EQ(descr.getKeyType(), KeyType::kString);

    std::vector<std::string> magicValues = recovery.retrieveMagicValues();

    ASSERT_EQ(magicValues.size(), 18u);

    ASSERT_EQ(magicValues[0], "RAFT_CURRENT_TERM: NotFound: ");
    ASSERT_EQ(magicValues[1], "RAFT_LOG_SIZE: NotFound: ");
    ASSERT_EQ(magicValues[2], "RAFT_LOG_START: NotFound: ");
    ASSERT_EQ(magicValues[3], "RAFT_CLUSTER_ID: NotFound: ");
    ASSERT_EQ(magicValues[4], "RAFT_VOTED_FOR: NotFound: ");
    ASSERT_EQ(magicValues[5], "RAFT_COMMIT_INDEX: NotFound: ");
    ASSERT_EQ(magicValues[6], "RAFT_MEMBERS: NotFound: ");
    ASSERT_EQ(magicValues[7], "RAFT_MEMBERSHIP_EPOCH: NotFound: ");
    ASSERT_EQ(magicValues[8], "RAFT_PREVIOUS_MEMBERS: NotFound: ");
    ASSERT_EQ(magicValues[9], "RAFT_PREVIOUS_MEMBERSHIP_EPOCH: NotFound: ");
    ASSERT_EQ(magicValues[10], "__format");
    ASSERT_EQ(magicValues[11], "0");
    ASSERT_EQ(magicValues[12], "__last-applied");
    ASSERT_EQ(magicValues[13], intToBinaryString(2));
    ASSERT_EQ(magicValues[14], "__in-bulkload");
    ASSERT_EQ(magicValues[15], boolToString(false));
    ASSERT_EQ(magicValues[16], "__clock");
    ASSERT_EQ(magicValues[17], unsignedIntToBinaryString(0u));
  }

  RedisRequest req {"recovery-get", "__last-applied"};
  ASSERT_EQ(Formatter::string(intToBinaryString(2)), RecoveryRunner::issueOneOffCommand("/tmp/quarkdb-recovery-test", req));
}

TEST(Recovery, RemoveJournalEntriesAndChangeClusterID) {
  {
    ASSERT_EQ(system("rm -rf /tmp/quarkdb-recovery-test"), 0);

    std::vector<RaftServer> nodes = {
      {"localhost", 1234},
      {"asdf", 2345},
      {"aaa", 999 }
    };

    RaftJournal journal("/tmp/quarkdb-recovery-test", "some-cluster-id", nodes, 0);
    ASSERT_TRUE(journal.setCurrentTerm(1, RaftServer()));
    ASSERT_TRUE(journal.append(1, RaftEntry(1, "set", "abc", "cdf")));

    ASSERT_TRUE(journal.setCurrentTerm(4, RaftServer()));
    ASSERT_TRUE(journal.append(2, RaftEntry(4, "set", "abc", "cdf")));

    ASSERT_EQ(journal.getLogSize(), 3);
  }

  {
    RecoveryRunner runner("/tmp/quarkdb-recovery-test", 15678);
    qclient::Options opts;
    opts.ensureConnectionIsPrimed = false;
    qclient::QClient qcl("localhost", 15678, std::move(opts) );

    ASSERT_REPLY(qcl.exec("recovery-get", KeyConstants::kJournal_ClusterID), "some-cluster-id");
    ASSERT_REPLY(qcl.exec("recovery-set", KeyConstants::kJournal_ClusterID, "different-cluster-id"), "OK");
    ASSERT_REPLY(qcl.exec("recovery-set", KeyConstants::kJournal_LogSize, intToBinaryString(2)), "OK");
    ASSERT_REPLY(qcl.exec("recovery-del", "does-not-exist"), "ERR Invalid argument: key not found, but I inserted a tombstone anyway. Deletion status: OK");
    ASSERT_REPLY(qcl.exec("recovery-get", SSTR("E" << intToBinaryString(2))), RaftEntry(4, "set", "abc", "cdf").serialize());
    ASSERT_REPLY(qcl.exec("recovery-del", SSTR("E" << intToBinaryString(2))), "OK");

    std::vector<std::string> rep = {
      "RAFT_CURRENT_TERM",
      intToBinaryString(4),
      "RAFT_LOG_SIZE",
      intToBinaryString(2),
      "RAFT_LOG_START",
      intToBinaryString(0),
      "RAFT_CLUSTER_ID",
      "different-cluster-id",
      "RAFT_VOTED_FOR",
      "",
      "RAFT_COMMIT_INDEX",
      intToBinaryString(0),
      "RAFT_MEMBERS",
      "localhost:1234,asdf:2345,aaa:999|",
      "RAFT_MEMBERSHIP_EPOCH",
      intToBinaryString(0),
      "RAFT_PREVIOUS_MEMBERS: NotFound: ",
      "RAFT_PREVIOUS_MEMBERSHIP_EPOCH: NotFound: ",
      "__format: NotFound: ",
      "__last-applied: NotFound: ",
      "__in-bulkload: NotFound: ",
      "__clock: NotFound: "
    };

    ASSERT_REPLY_DESCRIBE(qcl.exec("recovery-scan", "0", "COUNT", "2").get(),\
      "1) \"next:E\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\"\n"
      "2) 1) \"TYPE: value\"\n"
      "   2) \"KEY: E\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\"\n"
      "   3) \"VALUE: \\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x16\\x00\\x00\\x00\\x00\\x00\\x00\\x00JOURNAL_UPDATE_MEMBERS!\\x00\\x00\\x00\\x00\\x00\\x00\\x00localhost:1234,asdf:2345,aaa:999|\\x0F\\x00\\x00\\x00\\x00\\x00\\x00\\x00some-cluster-id\"\n"
      "   4) \"TYPE: value\"\n"
      "   5) \"KEY: E\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\"\n"
      "   6) \"VALUE: \\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\x00set\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\x00abc\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\x00cdf\"\n"
    );

    ASSERT_REPLY_DESCRIBE(qcl.exec("recovery-scan", "next:E\x00\x00\x00\x00\x00\x00\x00\x02", "COUNT", "2").get(),\
      "1) \"next:RAFT_COMMIT_INDEX\"\n"
      "2) 1) \"TYPE: deletion\"\n"
      "   2) \"KEY: E\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\"\n"
      "   3) \"VALUE: \"\n"
      "   4) \"TYPE: value\"\n"
      "   5) \"KEY: RAFT_CLUSTER_ID\"\n"
      "   6) \"VALUE: different-cluster-id\"\n"
    );

    ASSERT_REPLY(qcl.exec("recovery-info"), rep);

    ASSERT_REPLY(qcl.exec("recovery-force-reconfigure-journal", "test", "123"), "ERR cannot parse new members");
    ASSERT_REPLY(qcl.exec("recovery-force-reconfigure-journal", "example.com:99|", "awesome-cluster-id"), "OK");

    // Test integer <-> binary string conversion functions.
    redisReplyPtr conv1 = qcl.exec("convert-int-to-string", "999").get();
    ASSERT_EQ(qclient::describeRedisReply(conv1), "1) \"As int64_t: \\x00\\x00\\x00\\x00\\x00\\x00\\x03\\xE7\"\n2) \"As uint64_t: \\x00\\x00\\x00\\x00\\x00\\x00\\x03\\xE7\"\n");

    ASSERT_REPLY(qcl.exec("convert-int-to-string", "adfs"), "ERR cannot parse integer");
    ASSERT_REPLY(qcl.exec("convert-string-to-int", "qqqq"), "ERR expected string with 8 characters, was given 4 instead");

    redisReplyPtr conv2 = qcl.exec("convert-string-to-int", unsignedIntToBinaryString(999u)).get();
    ASSERT_EQ(qclient::describeRedisReply(conv2), "1) Interpreted as int64_t: 999\n2) Interpreted as uint64_t: 999\n");

    rep = {
      "RAFT_CURRENT_TERM",
      intToBinaryString(4),
      "RAFT_LOG_SIZE",
      intToBinaryString(2),
      "RAFT_LOG_START",
      intToBinaryString(0),
      "RAFT_CLUSTER_ID",
      "awesome-cluster-id",
      "RAFT_VOTED_FOR",
      "",
      "RAFT_COMMIT_INDEX",
      intToBinaryString(0),
      "RAFT_MEMBERS",
      "example.com:99|",
      "RAFT_MEMBERSHIP_EPOCH",
      intToBinaryString(0),
      "RAFT_PREVIOUS_MEMBERS: NotFound: ",
      "RAFT_PREVIOUS_MEMBERSHIP_EPOCH: NotFound: ",
      "__format: NotFound: ",
      "__last-applied: NotFound: ",
      "__in-bulkload: NotFound: ",
      "__clock: NotFound: "
    };

    ASSERT_REPLY(qcl.exec("recovery-info"), rep);
  }

  RaftJournal journal("/tmp/quarkdb-recovery-test");
  ASSERT_EQ(journal.getClusterID(), "awesome-cluster-id");
  ASSERT_EQ(journal.getLogSize(), 2);

  RaftEntry entry;
  ASSERT_NOTFOUND(journal.fetch(2, entry));
}
