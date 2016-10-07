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

#include "raft/RaftTalker.hh"
#include "Common.hh"
#include "Tunnel.hh"
#include "test-utils.hh"
#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
#define UNIX_SOCKET_PATH "/tmp/tunnel-unix-socket-2"

TEST(RaftTalker, T1) {
  unlink(UNIX_SOCKET_PATH);
  std::string clusterID = "b50da34e-ac15-4c02-b5a7-296454e5f779";
  RaftServer node = {"server3", 1234};
  RaftServer myself = {"its_me_ur_leader", 1337};
  Tunnel::addIntercept(node.hostname, node.port, UNIX_SOCKET_PATH);
  RaftTalker talker(node, clusterID);

  UnixSocketListener listener(UNIX_SOCKET_PATH);
  int s2 = listener.accept();
  ASSERT_GT(s2, 0);

  Link link(s2);
  XrdBuffManager bufferManager(NULL, NULL);
  RedisParser parser(&link, &bufferManager);

  RedisRequest req;

  int rc;
  while( (rc = parser.fetch(req)) == 0) ;
  ASSERT_EQ(rc, 1);

  RedisRequest tmp = {"RAFT_HANDSHAKE", clusterID};
  ASSERT_EQ(req, tmp);

  // send an append entries message over the talker
  std::vector<RedisRequest> requests;
  std::vector<RaftTerm> terms;

  // previous entry has higher term than myself
  ASSERT_THROW(talker.appendEntries(
               10, myself, // my state
               7, 11, // previous entry
               3, // commit index
               requests, terms // payload
             ), FatalException);

  requests.push_back( {"SET", "abc", "asdf"} );
  terms.push_back(3);

  requests.push_back( {"SET", "abcd", "1234"} );
  terms.push_back(12);

  requests.push_back( {"HSET", "myhash", "key", "value"});
  terms.push_back(12);

  // one of the entries has higher term than myself
  ASSERT_THROW(talker.appendEntries(
               11, myself, // my state
               7, 11, // previous entry
               3, // commit index
               requests, terms // payload
             ), FatalException);

  // valid request
  talker.appendEntries(
               12, myself, // my state
               7, 11, // previous entry
               3, // commit index
               requests, terms // payload
             );

  while( (rc = parser.fetch(req)) == 0) ;
  ASSERT_EQ(rc, 1);
  tmp = {"RAFT_APPEND_ENTRIES", "12", "its_me_ur_leader:1337", "7", "11", "3", "3",
         "3", "3", "SET", "abc", "asdf", // entry #1
         "3", "12", "SET", "abcd", "1234", // entry #2
         "4", "12", "HSET", "myhash", "key", "value" // entry #3
  };

  ASSERT_EQ(req, tmp);

  // terms in entries go down
  terms[terms.size()-1] = 11;
  ASSERT_THROW(talker.appendEntries(
               12, myself, // my state
               7, 11, // previous entry
               3, // commit index
               requests, terms // payload
             ), FatalException);

}
