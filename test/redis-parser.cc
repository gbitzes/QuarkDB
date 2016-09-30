// ----------------------------------------------------------------------
// File: redis-parser.cc
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

#include "RedisParser.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

class Redis_Parser : public ::testing::Test {
protected:
  virtual void SetUp() {
    bufferManager = new XrdBuffManager(NULL, NULL);
    parser = new RedisParser(&link, bufferManager);
  }

  virtual void TearDown() {
    delete parser;
    delete bufferManager;
  }

  void simulateBadConnection(const std::string &str, size_t bytes) {
    size_t i = 0;
    while(i + bytes < str.size()) {
      ASSERT_EQ(parser->fetch(request), 0);
      link.Send(str.c_str()+i, bytes);

      i += bytes;
    }

    // send any left-over bytes
    ASSERT_EQ(parser->fetch(request), 0);
    link.Send(str.c_str()+i, str.size()-i);
  }

  void simulateMany(const std::string &str, RedisRequest &valid, size_t bytes) {
    for(size_t i = 1; i < bytes; i++) {
      ASSERT_EQ(parser->fetch(request), 0);
      simulateBadConnection(str, i);
      ASSERT_EQ(parser->fetch(request), 1);
      ASSERT_EQ(request, valid);
    }
    ASSERT_EQ(parser->fetch(request), 0);
  }

  Link link;
  XrdBuffManager *bufferManager;
  RedisParser *parser;
  RedisRequest request;
};


TEST_F(Redis_Parser, T1) {
  ASSERT_EQ(parser->fetch(request), 0);
  link.Send("*2\r\n$3\r\nget\r\n$3\r\nabc\r\n*3\r\n$3\r\nset\r\n$3\r\nabc\r\n$5\r\nhello\r\n");
  ASSERT_EQ(parser->fetch(request), 1);
  ASSERT_EQ(request.size(), (size_t) 2);
  ASSERT_EQ(request[0], "get");
  ASSERT_EQ(request[1], "abc");

  request.clear();

  ASSERT_EQ(parser->fetch(request), 1);
  ASSERT_EQ(request.size(), (size_t) 3);
  ASSERT_EQ(request[0], "set");
  ASSERT_EQ(request[1], "abc");
  ASSERT_EQ(request[2], "hello");
}

TEST_F(Redis_Parser, T2) {
  std::string str("*2\r\n$3\r\nget\r\n$3\r\nabc\r\n");
  RedisRequest valid = {"get", "abc"};
  simulateMany(str, valid, 10);

  str = "*3\r\n$3\r\nset\r\n$4\r\nabcd\r\n$5\r\n12345\r\n";
  valid = {"set", "abcd", "12345"};
  simulateMany(str, valid, 10);

  // test two-digit integers
  str = "*3\r\n$3\r\nset\r\n$15\r\nthis_key_is_big\r\n$17\r\nthis_value_is_big\r\n";
  valid = {"set", "this_key_is_big", "this_value_is_big"};
  simulateMany(str, valid, 10);
}

TEST_F(Redis_Parser, T3) {
  // test bogus data
  link.Send("hello there\r\n");
  ASSERT_LT(parser->fetch(request), 0);
}

TEST_F(Redis_Parser, T4) {
  // bad integer
  link.Send("*lol\r\n");
  ASSERT_LT(parser->fetch(request), 0);
}

TEST_F(Redis_Parser, T5) {
  // wrong string size
  link.Send("*1\r\n$5\r\naaa\r\n");
  ASSERT_LE(parser->fetch(request), 0);
}

TEST_F(Redis_Parser, T6) {
  // bad string length integer
  link.Send("*1\r\n$asdf\r\n");
  ASSERT_LT(parser->fetch(request), 0);
}

TEST_F(Redis_Parser, T7) {
  // corrupted \r\n
  link.Send("*1\r\n$3abc\n\n");
  ASSERT_LT(parser->fetch(request), 0);
}

TEST_F(Redis_Parser, T8) {
  link.Send("*1\n\nabc");
  ASSERT_LT(parser->fetch(request), 0);
}
