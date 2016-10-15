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
#include "test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

#define ASSERT_REPLY(reply, val) { ASSERT_NE(reply, nullptr); ASSERT_EQ(std::string(((reply))->str, ((reply))->len), val); }



class tPoller : public TestCluster3Nodes {};

TEST_F(tPoller, T1) {
  RedisDispatcher dispatcher(*rocksdb());

  Poller rocksdbPoller(unixsocket(), &dispatcher);

  // start first connection
  Tunnel tunnel(myself().hostname, myself().port);

  redisReplyPtr reply = tunnel.execute({"set", "abc", "1234"}).get();
  ASSERT_REPLY(reply, "OK");

  reply = tunnel.execute({"get", "abc"}).get();
  ASSERT_REPLY(reply, "1234");

  // start second connection, ensure the poller can handle them concurrently
  Tunnel tunnel2(myself().hostname, myself().port);

  reply = tunnel2.execute({"get", "abc"}).get();
  ASSERT_REPLY(reply, "1234");

  reply = tunnel2.execute({"set", "qwert", "asdf"}).get();
  ASSERT_REPLY(reply, "OK");

  // now try a third
  Tunnel tunnel3(myself().hostname, myself().port);
  reply = tunnel3.execute({"get", "qwert"}).get();
  ASSERT_REPLY(reply, "asdf");
}

TEST_F(tPoller, test_reconnect) {
  RedisDispatcher dispatcher(*rocksdb());

  Tunnel tunnel(myself().hostname, myself().port);

  for(size_t reconnects = 0; reconnects < 5; reconnects++) {
    Poller rocksdbpoller(unixsocket(), &dispatcher);

    bool success = false;
    for(size_t i = 0; i < 30; i++) {
      redisReplyPtr reply = tunnel.execute({"set", "abc", "1234"}).get();
      if(reply != nullptr) {
        ASSERT_REPLY(reply, "OK");
        success = true;
        break;
      }
      else {
        ASSERT_FALSE(success);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    ASSERT_TRUE(success);
  }
}
