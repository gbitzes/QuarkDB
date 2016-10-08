// ----------------------------------------------------------------------
// File: poller.cc
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

#include "Poller.hh"
#include "Tunnel.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(Poller, T1) {
  RocksDB store("/tmp/rocksdb");
  RedisDispatcher dispatcher(store);

  unlink("/tmp/my-unix-socket");
  Tunnel::addIntercept("server1", 1234, "/tmp/my-unix-socket");
  Tunnel tunnel("server1", 1234);

  Poller poller("/tmp/my-unix-socket", &dispatcher);

  redisReplyPtr reply = tunnel.execute({"set", "abc", "1234"}).get();
  ASSERT_EQ(std::string(reply->str, reply->len), "OK");

  reply = tunnel.execute({"get", "abc"}).get();
  ASSERT_EQ(std::string(reply->str, reply->len), "1234");
}
