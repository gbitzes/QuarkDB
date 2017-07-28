// ----------------------------------------------------------------------
// File: background-flusher.cc
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

#include "raft/RaftJournal.hh"
#include "qclient/BackgroundFlusher.hh"
#include "../test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>
#include "../test-reply-macros.hh"

using namespace quarkdb;

using namespace quarkdb;
#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
class Background_Flusher : public TestCluster3NodesFixture {};

TEST_F(Background_Flusher, basic_sanity) {
  // start our cluster as usual
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();
  int follower = (leaderID + 1) % 3;

  qclient::QClient qcl(myself(follower).hostname, myself(follower).port, true, false);

  qclient::Notifier dummyNotifier;
  qclient::BackgroundFlusher flusher(qcl, dummyNotifier, 5000, 100);

  const int nentries = 10000;
  for(size_t i = 0; i <= nentries; i++) {
    flusher.pushRequest({"set", "key", SSTR("value-" << i)});
  }

  RETRY_ASSERT_TRUE(flusher.size() == 0u);
  ASSERT_TRUE(checkValueConsensus("key", SSTR("value-" << nentries), 0, 1, 2));

  // verify that every single request has been recorded
  LogIndex lastEntry = journal(leaderID)->getLogSize() - 1;
  LogIndex firstEntry = lastEntry - nentries;

  for(LogIndex index = lastEntry; index >= firstEntry; index--) {
    int64_t value = index - firstEntry;
    ASSERT_TRUE(checkJournalConsensus(index, make_req("set", "key", SSTR("value-" << value)), 0, 1, 2));
  }
}

TEST_F(Background_Flusher, failover) {
  // start our cluster as usual
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();
  int follower1 = (getLeaderID() + 1) % 3;
  int follower2 = (getLeaderID() + 2) % 3;

  qclient::Members members;
  members.push_back(myself(0).hostname, myself(0).port);
  members.push_back(myself(1).hostname, myself(1).port);
  members.push_back(myself(2).hostname, myself(2).port);

  qclient::QClient qcl(members, true, false);

  qclient::Notifier dummyNotifier;
  qclient::BackgroundFlusher flusher(qcl, dummyNotifier, 5000, 100);

  const int nentries = 10000;
  for(size_t i = 0; i <= nentries/2; i++) {
    flusher.pushRequest({"set", SSTR("key-" << i), SSTR("value-" << i)});
  }

  RETRY_ASSERT_TRUE(flusher.size() <= 2500u);
  spindown(leaderID);

  for(size_t i = nentries/2 + 1; i <= nentries; i++) {
    flusher.pushRequest({"set", SSTR("key-" << i), SSTR("value-" << i)});
  }

  RETRY_ASSERT_TRUE(flusher.size() == 0u);
  for(size_t i = 0; i <= nentries; i++) {
    ASSERT_TRUE(checkValueConsensus(SSTR("key-" << i), SSTR("value-" << i), follower1, follower2));
  }
}
