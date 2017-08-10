// ----------------------------------------------------------------------
// File: utils.cc
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

#include <gtest/gtest.h>
#include "raft/RaftCommon.hh"
#include "utils/IntToBinaryString.hh"
#include "utils/FileUtils.hh"
#include "utils/Resilvering.hh"
#include "Utils.hh"

using namespace quarkdb;

TEST(Utils, binary_string_int_conversion) {
  EXPECT_EQ(intToBinaryString(1), std::string("\x00\x00\x00\x00\x00\x00\x00\x01", 8));
  EXPECT_EQ(binaryStringToInt("\x00\x00\x00\x00\x00\x00\x00\x01"), 1);

  EXPECT_EQ(binaryStringToInt(intToBinaryString(1).data()), 1);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(2).c_str()), 2);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(123415).c_str()), 123415);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(17465798).c_str()), 17465798);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(16583415634).c_str()), 16583415634);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(-1234169761).c_str()), -1234169761);
}

TEST(Utils, binary_string_unsigned_int_conversion) {
  EXPECT_EQ(unsignedIntToBinaryString(1u), std::string("\x00\x00\x00\x00\x00\x00\x00\x01", 8));
  EXPECT_EQ(binaryStringToUnsignedInt("\x00\x00\x00\x00\x00\x00\x00\x01"), 1u);

  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(1u).data()), 1u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(2u).c_str()), 2u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(123415u).c_str()), 123415u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(17465798u).c_str()), 17465798u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(16583415634u).c_str()), 16583415634u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(18446744073709551613u).c_str()), 18446744073709551613u);

  uint64_t big_number = std::numeric_limits<uint64_t>::max() / 2;
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(big_number).c_str()), big_number);
}

TEST(Utils, pathJoin) {
  ASSERT_EQ(pathJoin("/home/", "test"), "/home/test");
  ASSERT_EQ(pathJoin("/home", "test"), "/home/test");
  ASSERT_EQ(pathJoin("", "home"), "/home");
  ASSERT_EQ(pathJoin("/home", ""), "/home");
}

TEST(Utils, resilvering_event_parsing) {
  ResilveringEvent event1("f493280d-009e-4388-a7ec-77ce66b77ce9", 123), event2;

  ASSERT_TRUE(ResilveringEvent::deserialize(event1.serialize(), event2));
  ASSERT_EQ(event1, event2);

  ASSERT_EQ(event1.getID(), event2.getID());
  ASSERT_EQ(event1.getStartTime(), event2.getStartTime());

  ResilveringEvent event3("a94a3955-be85-4e70-9fea-0f68eb01de89", 456);
  ASSERT_FALSE(event1 == event3);
}

TEST(Utils, resilvering_history_parsing) {
  ResilveringHistory history;

  history.append(ResilveringEvent("f493280d-009e-4388-a7ec-77ce66b77ce9", 123));
  history.append(ResilveringEvent("a94a3955-be85-4e70-9fea-0f68eb01de89", 456));
  history.append(ResilveringEvent("56f3dcec-2aa6-4487-b708-e867225d849c", 789));

  ResilveringHistory history2;
  ASSERT_TRUE(ResilveringHistory::deserialize(history.serialize(), history2));
  ASSERT_EQ(history, history2);

  for(size_t i = 0; i < history.size(); i++) {
    ASSERT_EQ(history.at(i), history2.at(i));
  }

  history2.append(ResilveringEvent("711e8894-ec4e-4f57-9c2c-eb9e260401ff", 890));
  ASSERT_FALSE(history == history2);

  ResilveringHistory history3, history4;
  ASSERT_TRUE(history3 == history4);
  ASSERT_FALSE(history == history3);
  ASSERT_FALSE(history3 == history);
}

TEST(Utils, replication_status) {
  ReplicationStatus status;
  ReplicaStatus replica { RaftServer("localhost", 123), true, 10000 };

  status.addReplica(replica);
  ASSERT_THROW(status.addReplica(replica), FatalException);

  replica.target = RaftServer("localhost", 456);
  replica.nextIndex = 20000;
  status.addReplica(replica);

  replica.target = RaftServer("localhost", 567);
  replica.online = false;
  status.addReplica(replica);

  ASSERT_EQ(status.replicasOnline(), 2u);
  ASSERT_EQ(status.replicasUpToDate(30000), 2u);
  ASSERT_EQ(status.replicasUpToDate(40001), 1u);
  ASSERT_EQ(status.replicasUpToDate(50001), 0u);

  ASSERT_THROW(status.removeReplica(RaftServer("localhost", 789)), FatalException);
  status.removeReplica(RaftServer("localhost", 456));
  ASSERT_EQ(status.replicasOnline(), 1u);
  ASSERT_EQ(status.replicasUpToDate(30000), 1u);

  ASSERT_EQ(status.getReplicaStatus(RaftServer("localhost", 123)).target, RaftServer("localhost", 123));
  ASSERT_THROW(status.getReplicaStatus(RaftServer("localhost", 456)).target, FatalException);
}
