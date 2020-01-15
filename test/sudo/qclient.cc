// ----------------------------------------------------------------------
// File: qclient.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "../test-utils.hh"
#include <qclient/QClient.hh>
#include <gtest/gtest.h>
#include <chrono>

using namespace quarkdb;

TEST(QClient, HostDroppingIncomingPacketsConstructor) {
  IptablesHelper iptables;

  ASSERT_TRUE(iptables.singleDropPackets(56789));

  qclient::Options opts;
  std::unique_ptr<qclient::QClient> qcl;

  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  qcl.reset(new qclient::QClient("localhost", 56789, std::move(opts)));
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

  std::chrono::milliseconds constructorDuration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
  std::cout << "Constructor took " << constructorDuration.count() << " ms" << std::endl;
  ASSERT_LE(constructorDuration, std::chrono::milliseconds(50));

  std::future<qclient::redisReplyPtr> reply = qcl->exec("PING");
  ASSERT_EQ(reply.wait_for(std::chrono::milliseconds(500)), std::future_status::timeout);

  start = std::chrono::steady_clock::now();
  qcl.reset();
  end = std::chrono::steady_clock::now();

  std::chrono::milliseconds destructorDuration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
  std::cout << "Destructor took " << destructorDuration.count() << " ms" << std::endl;
  ASSERT_LE(destructorDuration, std::chrono::milliseconds(50));

  ASSERT_TRUE(iptables.singleAcceptPackets(56789));
}

TEST(QClient, HostDroppingIncomingPacketsFutureTimeout) {
  IptablesHelper iptables;
  ASSERT_TRUE(iptables.singleDropPackets(56789));

  qclient::Options opts;
  opts.tcpTimeout = std::chrono::seconds(3);
  auto start = std::chrono::steady_clock::now();

  std::unique_ptr<qclient::QClient> qcl;
  qcl.reset(new qclient::QClient("localhost", 56789, std::move(opts)));
  qclient::redisReplyPtr reply = qcl->exec("PING").get();
  ASSERT_EQ(reply, nullptr);

  auto end = std::chrono::steady_clock::now();
  std::chrono::milliseconds dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Duration: " << dur.count() << std::endl;

  ASSERT_GE(dur, std::chrono::seconds(3));
  ASSERT_LE(dur, std::chrono::seconds(4));

  ASSERT_TRUE(iptables.singleAcceptPackets(56789));
}

