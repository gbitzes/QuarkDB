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

#include "raft/RaftUtils.hh"
#include <gtest/gtest.h>
using namespace quarkdb;

TEST(RaftParser, appendEntries1) {
  RedisRequest req = { "RAFT_APPEND_ENTRIES", "its_me_ur_leader:1338",
    intToBinaryString(12) + intToBinaryString(8) + intToBinaryString(10) +
    intToBinaryString(4) + intToBinaryString(4),
    RaftEntry(3, "SET", "abc", "12345").serialize(),
    RaftEntry(12, "SET", "4352", "adsfa").serialize(),
    RaftEntry(12, "HSET", "myhash", "key", "value").serialize(),
    RaftEntry(12, "UPDATE_RAFT_NODES", "server1:123,server2:321").serialize()
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
  RedisRequest req = { "RAFT_APPEND_ENTRIES", "its_me_ur_leader:1338",
    intToBinaryString(13) + intToBinaryString(9) + intToBinaryString(11) +
    intToBinaryString(7) + intToBinaryString(0)
  };

  RaftAppendEntriesRequest parsed;
  ASSERT_TRUE(RaftParser::appendEntries(std::move(req), parsed));

  ASSERT_EQ(parsed.term, 13);
  ASSERT_EQ(parsed.leader, RaftServer("its_me_ur_leader", 1338));
  ASSERT_EQ(parsed.prevIndex, 9);
  ASSERT_EQ(parsed.prevTerm, 11);
  ASSERT_EQ(parsed.commitIndex, 7);

  ASSERT_EQ(parsed.entries.size(), 0u);
}
