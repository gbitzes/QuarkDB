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

  RecoveryEditor recovery("/tmp/quarkdb-recovery-test");

  std::string val;
  ASSERT_OK(recovery.get(DescriptorLocator("abc").toString(), val));
  KeyDescriptor descr(val);
  ASSERT_EQ(descr.getKeyType(), KeyType::kString);

  std::vector<std::string> magicValues = recovery.retrieveMagicValues();

  ASSERT_EQ(magicValues.size(), 16u);

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
}

TEST(Recovery, RemoveJournalEntriesAndChangeClusterID) {
  {
    ASSERT_EQ(system("rm -rf /tmp/quarkdb-recovery-test"), 0);

    std::vector<RaftServer> nodes = {
      {"localhost", 1234},
      {"asdf", 2345},
      {"aaa", 999 }
    };

    RaftJournal journal("/tmp/quarkdb-recovery-test", "some-cluster-id", nodes);
    ASSERT_TRUE(journal.setCurrentTerm(1, RaftServer()));
    ASSERT_TRUE(journal.append(1, RaftEntry(1, "set", "abc", "cdf")));

    ASSERT_TRUE(journal.setCurrentTerm(4, RaftServer()));
    ASSERT_TRUE(journal.append(2, RaftEntry(4, "set", "abc", "cdf")));

    ASSERT_EQ(journal.getLogSize(), 3);
  }

  {
    RecoveryRunner runner("/tmp/quarkdb-recovery-test", 30100);
    qclient::QClient qcl("localhost", 30100);

    ASSERT_REPLY(qcl.exec("get", KeyConstants::kJournal_ClusterID), "some-cluster-id");
    ASSERT_REPLY(qcl.exec("set", KeyConstants::kJournal_ClusterID, "different-cluster-id"), "OK");
    ASSERT_REPLY(qcl.exec("set", KeyConstants::kJournal_LogSize, intToBinaryString(2)), "OK");
    ASSERT_REPLY(qcl.exec("del", "does-not-exist"), "ERR Invalid argument: key not found, but I inserted a tombstone anyway. Deletion status: OK");
    ASSERT_REPLY(qcl.exec("get", SSTR("E" << intToBinaryString(2))), RaftEntry(4, "set", "abc", "cdf").serialize());
    ASSERT_REPLY(qcl.exec("del", SSTR("E" << intToBinaryString(2))), "OK");

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
      "__in-bulkload: NotFound: "
    };

    ASSERT_REPLY(qcl.exec("recovery-info"), rep);
  }

  RaftJournal journal("/tmp/quarkdb-recovery-test");
  ASSERT_EQ(journal.getClusterID(), "different-cluster-id");
  ASSERT_EQ(journal.getLogSize(), 2);

  RaftEntry entry;
  ASSERT_NOTFOUND(journal.fetch(2, entry));
}
