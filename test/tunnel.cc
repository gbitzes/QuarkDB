// ----------------------------------------------------------------------
// File: tunnel.cc
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

#include "Tunnel.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>


using namespace quarkdb;

static void assert_receive(int fd, const std::string &contents) {
  char buffer[contents.size()];
  int len = recv(fd, buffer, contents.size(), 0);
  EXPECT_EQ(len, (int) contents.size());
  ASSERT_EQ(std::string(buffer, len), contents);
}

static void socket_send(int fd, const std::string &contents) {
  ASSERT_GT(send(fd, contents.c_str(), contents.size(), 0), 0);
}

static std::string str_from_reply(redisReplyPtr &reply) {
  return std::string(reply->str, reply->len);
}

TEST(Tunnel, T1) {
  Tunnel tunnel("localhost", 1234);

  RedisRequest req { "set", "abc", "123" };
  std::future<redisReplyPtr> fut = tunnel.execute(req);
  ASSERT_EQ(fut.get(), nullptr);

  SocketListener listener(1234);
  int s2 = listener.accept();
  ASSERT_GT(s2, 0);

  // connected
  fut = tunnel.execute(req);

  assert_receive(s2, "*3\r\n$3\r\nset\r\n$3\r\nabc\r\n$3\r\n123\r\n");
  socket_send(s2, "+OK\r\n");

  redisReplyPtr reply = fut.get();
  ASSERT_EQ(reply->type, REDIS_REPLY_STATUS);
  ASSERT_EQ(str_from_reply(reply), "OK");

  req = { "get", "abc" };
  fut = tunnel.execute(req);

  req = { "get", "qwerty" };
  std::future<redisReplyPtr> fut2 = tunnel.execute(req);

  assert_receive(s2, "*2\r\n$3\r\nget\r\n$3\r\nabc\r\n");
  assert_receive(s2, "*2\r\n$3\r\nget\r\n$6\r\nqwerty\r\n");

  socket_send(s2, "$-1\r\n");
  socket_send(s2, "$7\r\n1234567\r\n");

  reply = fut.get();
  ASSERT_EQ(reply->type, REDIS_REPLY_NIL);
  reply = fut2.get();
  ASSERT_EQ(reply->type, REDIS_REPLY_STRING);
  ASSERT_EQ(str_from_reply(reply), "1234567");
  close(s2);
}

TEST(Tunnel, T2) {
  // with handshake
  Tunnel tunnel("localhost", 1234, {"RAFT_HANDSHAKE", "some-cluster-id"});

  RedisRequest req { "set", "abc", "123" };
  std::future<redisReplyPtr> fut = tunnel.execute(req);
  ASSERT_EQ(fut.get(), nullptr);

  SocketListener listener(1234);
  int s2 = listener.accept();
  ASSERT_GT(s2, 0);

  // connected
  fut = tunnel.execute(req);
  assert_receive(s2, "*2\r\n$14\r\nRAFT_HANDSHAKE\r\n$15\r\nsome-cluster-id\r\n");
  socket_send(s2, "+OK\r\n");

  assert_receive(s2, "*3\r\n$3\r\nset\r\n$3\r\nabc\r\n$3\r\n123\r\n");
  socket_send(s2, "+OK\r\n");
  close(s2);
}
