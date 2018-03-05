// ----------------------------------------------------------------------
// File: multi.cc
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

#include "redis/MultiOp.hh"
#include "test-utils.hh"
#include "test-reply-macros.hh"
#include <gtest/gtest.h>
#include <qclient/QClient.hh>

using namespace quarkdb;
using namespace qclient;

class Multi : public TestCluster3NodesFixture {};

TEST_F(Multi, Dispatching) {
  RedisDispatcher dispatcher(*stateMachine());

  MultiOp multi1;
  multi1.emplace_back("GET", "aaa");
  multi1.emplace_back("SET", "aaa", "bbb");
  multi1.emplace_back("GET", "aaa");

  RedisEncodedResponse resp = dispatcher.dispatch(multi1, 1);
  ASSERT_EQ(resp.val, "*3\r\n$-1\r\n+OK\r\n$3\r\nbbb\r\n");

  RedisRequest req = {"GET", "aaa"};
  resp = dispatcher.dispatch(req, 0);
  ASSERT_EQ(resp.val, "$3\r\nbbb\r\n");
}
