// ----------------------------------------------------------------------
// File: raft-parser.cc
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

#include "raft/RaftParser.hh"
#include <gtest/gtest.h>
using namespace quarkdb;

TEST(RaftParser, appendEntries1) {
  RedisRequest req = { "RAFT_APPEND_ENTRIES", "12", "its_me_ur_leader:1338", "8", "10", "4", "4",
    "3", "3", "SET", "abc", "12345", // entry #1
    "3", "12", "SET", "4352", "adsfa", // entry #2
    "4", "12", "HSET", "myhash", "key", "value", // entry #3
    "2", "12", "UPDATE_RAFT_NODES", "server1:123,server2:321" // entry #4
  };

  RaftAppendEntriesRequest parsed;
  ASSERT_TRUE(RaftParser::appendEntries(std::move(req), parsed));

  ASSERT_EQ(parsed.term, 12);
  ASSERT_EQ(parsed.leader, RaftServer("its_me_ur_leader", 1338));
  ASSERT_EQ(parsed.prevIndex, 8);
  ASSERT_EQ(parsed.prevTerm, 10);
  ASSERT_EQ(parsed.commitIndex, 4);

  ASSERT_EQ(parsed.entries.size(), 4u);

  RedisRequest tmp = {"SET", "abc", "12345"};
  ASSERT_EQ(parsed.entries[0].request, tmp);
  ASSERT_EQ(parsed.entries[0].term, 3);

  tmp = {"SET", "4352", "adsfa"};
  ASSERT_EQ(parsed.entries[1].request, tmp);
  ASSERT_EQ(parsed.entries[1].term, 12);

  tmp = {"HSET", "myhash", "key", "value"};
  ASSERT_EQ(parsed.entries[2].request, tmp);
  ASSERT_EQ(parsed.entries[2].term ,12);

  tmp = {"UPDATE_RAFT_NODES", "server1:123,server2:321"};
  ASSERT_EQ(parsed.entries[3].request, tmp);
  ASSERT_EQ(parsed.entries[3].term, 12);
}

TEST(RaftParser, appendEntries2) {
  // heartbeat
  RedisRequest req = { "RAFT_APPEND_ENTRIES", "13", "its_me_ur_leader:1338", "9", "11", "7", "0"};

  RaftAppendEntriesRequest parsed;
  ASSERT_TRUE(RaftParser::appendEntries(std::move(req), parsed));

  ASSERT_EQ(parsed.term, 13);
  ASSERT_EQ(parsed.leader, RaftServer("its_me_ur_leader", 1338));
  ASSERT_EQ(parsed.prevIndex, 9);
  ASSERT_EQ(parsed.prevTerm, 11);
  ASSERT_EQ(parsed.commitIndex, 7);

  ASSERT_EQ(parsed.entries.size(), 0u);
}

TEST(RaftParser, appendEntries3) {
  // malformed entries
  RedisRequest req = { "RAFT_APPEND_ENTRIES", "13", "no_port_in_leader", "9", "11", "7", "0"};
  RaftAppendEntriesRequest parsed;
  ASSERT_FALSE(RaftParser::appendEntries(std::move(req), parsed));

  req = { "RAFT_APPEND_ENTRIES", "13", "its_me_ur_leader:1338", "9", "11", "7", "1",
          "4", "12", "SET", "abc", "789" // screwed up reqsize
  };

  ASSERT_FALSE(RaftParser::appendEntries(std::move(req), parsed));

  req = { "RAFT_APPEND_ENTRIES", "13", "its_me_ur_leader:1338", "9", "11", "7", "2", // wrong number of requests
          "3", "12", "SET", "abc", "789"
  };

  ASSERT_FALSE(RaftParser::appendEntries(std::move(req), parsed));

  req = { "RAFT_APPEND_ENTRIES", "13", "its_me_ur_leader:1338", "9", "11", "7", "1",
          "3", "12", "SET", "abc", "789", "wheee" // extra chunk
  };

  ASSERT_FALSE(RaftParser::appendEntries(std::move(req), parsed));
}
