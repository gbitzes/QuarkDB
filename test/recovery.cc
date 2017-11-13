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

#include "recovery/RecoveryEditor.hh"
#include "storage/KeyLocators.hh"
#include "StateMachine.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())

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

  ASSERT_EQ(magicValues.size(), 14u);

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
}
