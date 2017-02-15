// ----------------------------------------------------------------------
// File: hset.cc
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

#include "StateMachine.hh"
#include "../test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

class hset : public TestCluster3Nodes, public ::testing::TestWithParam<int> {
public:
  void processRangeRedis(size_t start, size_t end, int port) {
    qclient::QClient tunn("localhost", port);
    for(size_t i = start; i < end; i++) {
      tunn.exec("hset", SSTR("key-" << i), "field", "some_contents");
    }
    tunn.exec("ping").get(); // receive all responses
  }

  void processRange(size_t start, size_t end) {
    for(size_t i = start; i < end; i++) {
      bool created;
      stateMachine()->hset(SSTR("key-" << i), "field", "some_contents", created, 0);
    }
  }
};

INSTANTIATE_TEST_CASE_P(TestWithMultipleThreads,
                        hset,
                        ::testing::Values(1, 2, 4, 8),
                        ::testing::PrintToStringParamName());

TEST_P(hset, directly_without_redis_protocol) {
  size_t number_of_inserts = 1000000;
  stateMachine(); // initialize

  auto startTime = std::chrono::high_resolution_clock::now();

  qdb_info("Starting benchmark: issue HSET " << number_of_inserts << " times directly to the state machine, no redis, " << GetParam() << " threads");

  std::vector<std::thread> threads;
  for(size_t i = 0; i < (size_t) GetParam(); i++) {
    size_t start = (number_of_inserts / GetParam()) * i;
    size_t end = (number_of_inserts / GetParam()) * (i+1);

    threads.emplace_back(&hset::processRange, this, start, end);
    qdb_info("Thread #" << i << " was assigned to range [" << start << "-" << end << ")");
  }

  for(size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  qdb_info("Benchmark has ended");

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime).count();
  qdb_info("Rate: " << (float) number_of_inserts / ((float) duration / (float) 1000) << " Hz");
}

TEST_P(hset, through_redis_protocol) {
  size_t number_of_inserts = 1000000;
  stateMachine(); // initialize

  auto startTime = std::chrono::high_resolution_clock::now();

  RedisDispatcher disp(*stateMachine());
  int port = 34567;
  Poller poll(port, &disp);
  qclient::QClient tunn("localhost", port);

  qdb_info("Starting benchmark: issue HSET " << number_of_inserts << " times through the redis protocol, " << GetParam() << " threads");
  std::vector<std::thread> threads;
  for(size_t i = 0; i < (size_t) GetParam(); i++) {
    size_t start = (number_of_inserts / GetParam()) * i;
    size_t end = (number_of_inserts / GetParam()) * (i+1);

    threads.emplace_back(&hset::processRangeRedis, this, start, end, port);
    qdb_info("Thread #" << i << " was assigned to range [" << start << "-" << end << ")");
  }

  for(size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  qdb_info("Benchmark has ended");

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime).count();
  qdb_info("Rate: " << (float) number_of_inserts / ((float) duration / (float) 1000) << " Hz");
}
