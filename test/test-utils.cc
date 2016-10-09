// ----------------------------------------------------------------------
// File: test-utils.cc
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

#include "test-utils.hh"
#include "Utils.hh"
#include "Tunnel.hh"
#include <vector>
#include <string>

namespace quarkdb {

std::vector<RedisRequest> testreqs = {
  {"set", "abc", "123"},
  {"set", "123", "abc"},
  {"hset", "myhash", "value", "234" },
  {"sadd", "myset", "a"},
  {"sadd", "myset", "b"},
  {"sadd", "myset", "c"},
  {"sadd", "myset", "d"}
};

std::vector<RaftServer> testnodes3;

std::vector<RaftServer> testnodes = {
  {"server0", 7775},
  {"server1", 7776},
  {"server2", 7777},
  {"server3", 7778},
  {"server4", 7779}
};

std::vector<RaftClusterID> test_clusterIDs {
  "8b4651de-b37c-4f6f-9738-1125998c680e",
  "a9b9e979-5428-42e9-8a52-f675c39fdf80",
  "6d114e5f-5e19-4a01-b200-a837bc972ad2",
  "5c29d090-c56e-4b7a-991c-3ba41a4e094b",
  "e91e9315-a0dc-4498-8edf-209abd2ef8e2"
};

std::vector<std::string> test_unixsockets;
std::vector<std::string> test_journals;
std::vector<std::string> test_statemachines;

TestsCommonState::TestsCommonState() {
  system("rm -r /tmp/quarkdb-tests");
  system("mkdir /tmp/quarkdb-tests");

  for(size_t i = 0; i < 3; i++) {
    testnodes3.push_back(testnodes[i]);
  }

  for(size_t i = 0; i < testnodes.size(); i++) {
    test_unixsockets.push_back(SSTR("/tmp/quarkdb-tests/unix-socket-" << testnodes[i].hostname << "-" << testnodes[i].port));
    Tunnel::addIntercept(testnodes[i].hostname, testnodes[i].port, test_unixsockets[i]);

    test_journals.push_back(SSTR("/tmp/quarkdb-tests/journal-" << testnodes[i].hostname << "-" << testnodes[i].port));
    test_statemachines.push_back(SSTR("/tmp/quarkdb-tests/statemachine-" << testnodes[i].hostname << "-" << testnodes[i].port));
  }
}
TestsCommonState commonState;



}
