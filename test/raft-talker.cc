// ----------------------------------------------------------------------
// File: raft-talker.cc
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

#include "utils/IntToBinaryString.hh"
#include "raft/RaftTalker.hh"
#include "raft/RaftContactDetails.hh"
#include "Common.hh"
#include "Version.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(RaftTalker, T1) {
  std::string clusterID = "b50da34e-ac15-4c02-b5a7-296454e5f779";
  RaftTimeouts timeouts(std::chrono::milliseconds(1), std::chrono::milliseconds(2),
    std::chrono::milliseconds(3));
  RaftServer node = {"localhost", 12344};
  RaftServer myself = {"its_me_ur_leader", 1337};
  RaftContactDetails cd(clusterID, timeouts);
  RaftTalker talker(node, cd);

  SocketListener listener(12344);
  int s2 = listener.accept();
  ASSERT_GT(s2, 0);

  Link link(s2);
  RedisParser parser(&link);

  RedisRequest req;

  int rc;
  while( (rc = parser.fetch(req)) == 0) ;
  ASSERT_EQ(rc, 1);

  RedisRequest tmp = {"RAFT_HANDSHAKE", VERSION_FULL_STRING, clusterID, timeouts.toString()};
  ASSERT_EQ(req, tmp);
  link.Send("+OK\r\n");

  // send an append entries message over the talker
  std::vector<RaftSerializedEntry> entries;

  entries.emplace_back(RaftEntry(3, "SET", "abc", "asdf").serialize());
  entries.emplace_back(RaftEntry(12, "SET", "abcd", "1234").serialize());
  entries.emplace_back(RaftEntry(12, "HSET", "myhash", "key", "value").serialize());

  // valid request
  talker.appendEntries(
               12, myself, // my state
               7, 11, // previous entry
               3, // commit index
               entries // payload
             );

  while( (rc = parser.fetch(req)) == 0) ;
  ASSERT_EQ(rc, 1);
  tmp = {"RAFT_APPEND_ENTRIES", "its_me_ur_leader:1337",
         intToBinaryString(12) + intToBinaryString(7) + intToBinaryString(11) +
         intToBinaryString(3) + intToBinaryString(3),
         entries[0],
         entries[1],
         entries[2]
  };

  ASSERT_EQ(req, tmp);
}
