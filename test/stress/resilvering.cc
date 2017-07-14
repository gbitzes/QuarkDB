// ----------------------------------------------------------------------
// File: resilvering.cc
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

#include "../test-reply-macros.hh"
#include "../test-utils.hh"
#include "raft/RaftConfig.hh"

using namespace quarkdb;
class Trimming : public TestCluster3NodesFixture {};

TEST_F(Trimming, configurable_trimming_limit) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();

  std::vector<std::future<redisReplyPtr>> futures;

  // push lots of updates
  const int64_t NENTRIES = 500;
  for(size_t i = 0; i < NENTRIES; i++) {
    futures.emplace_back(tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i)));
  }

  // Set journal trim config to ridiculously low values.
  // This is to ensure the trimmer never tries to remove non-committed or non-applied entries.
  // With a sane trim limit in the millions, this would never happen anyway, but let's be paranoid.
  Link link;
  Connection dummy(&link);
  TrimmingConfig trimConfig { 2, 1 };
  raftconfig(leaderID)->setTrimmingConfig(&dummy, trimConfig, true);

  // some more updates...
  for(size_t i = NENTRIES; i < NENTRIES*2; i++) {
    futures.emplace_back(tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i)));
  }

  // Get responses
  for(size_t i = 0; i < futures.size(); i++) {
    ASSERT_REPLY(futures[i], "OK");
  }

  RETRY_ASSERT_TRUE(journal(0)->getLogStart() == 1000);
  RETRY_ASSERT_TRUE(journal(1)->getLogStart() == 1000);
  RETRY_ASSERT_TRUE(journal(2)->getLogStart() == 1000);

  RETRY_ASSERT_TRUE(stateMachine(0)->getLastApplied() == 1002);
  RETRY_ASSERT_TRUE(stateMachine(1)->getLastApplied() == 1002);
  RETRY_ASSERT_TRUE(stateMachine(2)->getLastApplied() == 1002);
}
