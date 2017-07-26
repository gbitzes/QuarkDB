// ----------------------------------------------------------------------
// File: connection.cc
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

#include "Connection.hh"
#include "RedisParser.hh"
#include "StateMachine.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

class tConnection : public TestCluster3NodesFixture {};

TEST_F(tConnection, basic_sanity) {
  const int BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  RedisDispatcher dispatcher(*stateMachine());

  Link link;
  Connection conn(&link);
  conn.setResponseBuffering(false);

  conn.addPendingRequest(&dispatcher, {"get", "abc"});
  int len = link.Recv(buffer, BUFFER_SIZE, 0);
  ASSERT_EQ(std::string(buffer, len), "$-1\r\n");

  conn.err("fatality");
  len = link.Recv(buffer, BUFFER_SIZE, 0);
  ASSERT_EQ(std::string(buffer, len), "-ERR fatality\r\n");

  conn.addPendingRequest(&dispatcher, {"set", "abc", "qwerty"}, 1);
  ASSERT_THROW(conn.addPendingRequest(&dispatcher, {"set", "abc", "qwerty"}, 1), FatalException);

  // verify the request has NOT been dispatched yet
  std::string tmp;
  ASSERT_FALSE(stateMachine()->get("abc", tmp).ok());

  conn.addPendingRequest(&dispatcher, {"get", "abc"});
  ASSERT_EQ(link.Recv(buffer, BUFFER_SIZE, 0), 0); // "set" is blocking any replies
  conn.addPendingRequest(&dispatcher, {"ping"});
  conn.addPendingRequest(&dispatcher, {"set", "abc", "12345"}, 2);

  ASSERT_EQ(conn.dispatchPending(&dispatcher, 1), 2);
  len = link.Recv(buffer, BUFFER_SIZE, 0);
  ASSERT_EQ(std::string(buffer, len), "+OK\r\n$6\r\nqwerty\r\n+PONG\r\n");

  conn.err("fatality^2");
  conn.addPendingRequest(&dispatcher, {"get", "abc"});
  ASSERT_EQ(link.Recv(buffer, BUFFER_SIZE, 0), 0); // "set" is blocking any replies

  ASSERT_TRUE(stateMachine()->get("abc", tmp).ok());
  ASSERT_EQ(tmp, "qwerty");

  ASSERT_EQ(conn.dispatchPending(&dispatcher, 2), -1);

  ASSERT_TRUE(stateMachine()->get("abc", tmp).ok());
  ASSERT_EQ(tmp, "12345");

  len = link.Recv(buffer, BUFFER_SIZE, 0);
  ASSERT_EQ(std::string(buffer, len), "+OK\r\n-ERR fatality^2\r\n$5\r\n12345\r\n");

  ASSERT_THROW(conn.addPendingRequest(&dispatcher, {"set", "asdf", "qwerty"}, 1), FatalException);
}
