// ----------------------------------------------------------------------
// File: poweroff.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "../test-utils.hh"
#include "../test-reply-macros.hh"
#include <qclient/QClient.hh>
#include <chrono>

using namespace quarkdb;
class Poweroff : public TestCluster3NodesFixture {};

TEST_F(Poweroff, WithDataLoss) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));

  int leaderID = getLeaderID();

  std::vector<std::future<redisReplyPtr>> vec;
  for(size_t i = 0; i < 10000; i++) {
    vec.emplace_back(tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-" << i)));
  }

  for(size_t i = 0; i < 10000; i++) {
    ASSERT_REPLY_DESCRIBE(vec[i].get(), "OK");
  }

  int follower = (leaderID + 1) % 3;
  int followerPort = nodes()[follower].port;

  ASSERT_EQ(system(SSTR("iptables -I OUTPUT -p tcp --dest 127.0.0.1 --dport " << followerPort << " -j DROP").c_str()), 0);
  spindown(follower);

  ASSERT_TRUE(journal(follower)->simulateDataLoss(3));
  ASSERT_EQ(journal(follower)->getLogSize(), journal(leaderID)->getLogSize() - 3);

  ASSERT_EQ(system(SSTR("iptables -I OUTPUT -p tcp --dest 127.0.0.1 --dport " << followerPort << " -j ACCEPT").c_str()), 0);
  spinup(follower);

  // ensure the leader restores the missing entries
  RETRY_ASSERT_EQ(journal(follower)->getLogSize(), journal(leaderID)->getLogSize());
}

