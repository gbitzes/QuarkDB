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
#include "test-utils.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>
#include <qclient/ReconnectionListener.hh>

using namespace quarkdb;
using namespace qclient;

#define ASSERT_REPLY(reply, val) { ASSERT_NE(reply, nullptr); ASSERT_EQ(std::string(((reply))->str, ((reply))->len), val); }

class tPoller : public TestCluster3NodesFixture {};

TEST_F(tPoller, T1) {
  RedisDispatcher dispatcher(*stateMachine());

  Poller smPoller(myself().port, &dispatcher);

  // start first connection
  QClient tunnel(myself().hostname, myself().port, {} );

  redisReplyPtr reply = tunnel.exec("set", "abc", "1234").get();
  ASSERT_REPLY(reply, "OK");

  reply = tunnel.exec("get", "abc").get();
  ASSERT_REPLY(reply, "1234");

  // start second connection, ensure the poller can handle them concurrently
  QClient tunnel2(myself().hostname, myself().port, {} );

  reply = tunnel2.exec("get", "abc").get();
  ASSERT_REPLY(reply, "1234");

  reply = tunnel2.exec("set", "qwert", "asdf").get();
  ASSERT_REPLY(reply, "OK");

  // now try a third
  QClient tunnel3(myself().hostname, myself().port, {} );
  reply = tunnel3.exec("get", "qwert").get();
  ASSERT_REPLY(reply, "asdf");
}


class ReconnectionCounter : public ReconnectionListener {
public:

  virtual void notifyConnectionLost(int64_t epoch, int errc,
    const std::string &msg) override { }

  virtual void notifyConnectionEstablished(int64_t epoch) override {
    lastEpoch = epoch;
  }

  int64_t getEpoch() const {
    return lastEpoch;
  }

private:
  int64_t lastEpoch = 0u;
};

TEST_F(tPoller, test_reconnect) {
  RedisDispatcher dispatcher(*stateMachine());

  std::shared_ptr<ReconnectionCounter> listener = std::make_shared<ReconnectionCounter>();

  qclient::Options opts;
  opts.reconnectionListener = listener;

  QClient tunnel(myself().hostname, myself().port, std::move(opts));

  for(size_t reconnects = 0; reconnects < 5; reconnects++) {
    Poller rocksdbpoller(myself().port, &dispatcher);

    bool success = false;
    for(size_t i = 0; i < 30; i++) {
      redisReplyPtr reply = tunnel.exec("set", "abc", "1234").get();
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

  std::cout << "Number of reconnections in total: " << listener->getEpoch() << std::endl;
  ASSERT_GE(listener->getEpoch(), 6u);
}
