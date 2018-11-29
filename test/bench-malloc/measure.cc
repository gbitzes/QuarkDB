// ----------------------------------------------------------------------
// File: measure.cc
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

#include "interceptor.hh"
#include <gtest/gtest.h>
#include "../test-utils.hh"
#include <qclient/QClient.hh>
#include "../test-reply-macros.hh"

using namespace quarkdb;
using namespace qclient;
class AllocationCount : public TestCluster3NodesFixture {};

TEST_F(AllocationCount, 300k_entries) {
  spinup(0); spinup(1); spinup(2);
  RETRY_ASSERT_TRUE(checkStateConsensus(0, 1, 2));
  int leaderID = getLeaderID();

  int64_t startAllocations = getAllocationCount();
  int64_t startFrees = getFreeCount();

  std::vector<std::future<redisReplyPtr>> futures;

  constexpr int64_t NENTRIES = 300000;
  for(size_t i = 0; i < NENTRIES; i++) {
    futures.emplace_back(tunnel(leaderID)->exec("set", SSTR("key-" << i), SSTR("value-------------------------------------------------------------------" << i)));
  }

  for(size_t i = 0; i < futures.size(); i++) {
    ASSERT_REPLY(futures[i], "OK");
  }

  int64_t endAllocations = getAllocationCount();
  int64_t endFrees = getFreeCount();

  int64_t diffAllocations = endAllocations - startAllocations;
  int64_t diffFrees = endFrees - startFrees;

  qdb_info("-------------------- Allocations per entry: " << (double) diffAllocations / (double) NENTRIES);
  qdb_info("-------------------- Frees per entry: " << (double) diffFrees / (double) NENTRIES);
}
