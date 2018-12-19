// ----------------------------------------------------------------------
// File: response-formatter.cc
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

#include "Formatter.hh"
#include "redis/ArrayResponseBuilder.hh"
#include "qclient/ResponseBuilder.hh"
#include "qclient/QClient.hh"
#include <gtest/gtest.h>

using namespace quarkdb;
using namespace qclient;

TEST(Response, T1) {
  ASSERT_EQ(Formatter::err("test").val, "-ERR test\r\n");
  ASSERT_EQ(Formatter::ok().val, "+OK\r\n");
  ASSERT_EQ(Formatter::pong().val, "+PONG\r\n");
  ASSERT_EQ(Formatter::null().val, "$-1\r\n");
  ASSERT_EQ(Formatter::status("test").val, "+test\r\n");
  ASSERT_EQ(Formatter::noauth("asdf").val, "-NOAUTH asdf\r\n");
  ASSERT_EQ(
    Formatter::multiply(Formatter::noauth("you shall not pass"), 3).val,
    "-NOAUTH you shall not pass\r\n-NOAUTH you shall not pass\r\n-NOAUTH you shall not pass\r\n"
  );
}

TEST(ArrayResponseBuilder, BasicSanity) {
  ArrayResponseBuilder builder(3, false);
  ASSERT_THROW(builder.buildResponse(), FatalException);

  builder.push_back(Formatter::ok());
  builder.push_back(Formatter::integer(999));
  builder.push_back(Formatter::string("whee"));
  ASSERT_THROW(builder.push_back(Formatter::integer(123)), FatalException);

  RedisEncodedResponse resp = builder.buildResponse();
  ASSERT_EQ(resp.val, "*3\r\n+OK\r\n:999\r\n$4\r\nwhee\r\n");
}

TEST(Formatter, subscribe) {
  qclient::ResponseBuilder builder;
  builder.feed(Formatter::subscribe("channel-name", 3).val);

  redisReplyPtr ans;
  ASSERT_EQ(builder.pull(ans), qclient::ResponseBuilder::Status::kOk);

  ASSERT_EQ(qclient::describeRedisReply(ans),
    "1) \"subscribe\"\n"
    "2) \"channel-name\"\n"
    "3) (integer) 3\n");
}

TEST(Formatter, psubscribe) {
  qclient::ResponseBuilder builder;
  builder.feed(Formatter::psubscribe("channel-*", 4).val);

  redisReplyPtr ans;
  ASSERT_EQ(builder.pull(ans), qclient::ResponseBuilder::Status::kOk);

  ASSERT_EQ(qclient::describeRedisReply(ans),
    "1) \"psubscribe\"\n"
    "2) \"channel-*\"\n"
    "3) (integer) 4\n");
}

TEST(Formatter, unsubscribe) {
  qclient::ResponseBuilder builder;
  builder.feed(Formatter::unsubscribe("channel-name", 5).val);

  redisReplyPtr ans;
  ASSERT_EQ(builder.pull(ans), qclient::ResponseBuilder::Status::kOk);

  ASSERT_EQ(qclient::describeRedisReply(ans),
    "1) \"unsubscribe\"\n"
    "2) \"channel-name\"\n"
    "3) (integer) 5\n");
}

TEST(Formatter, message) {
  qclient::ResponseBuilder builder;
  builder.feed(Formatter::message("channel", "payload").val);

  redisReplyPtr ans;
  ASSERT_EQ(builder.pull(ans), qclient::ResponseBuilder::Status::kOk);

  ASSERT_EQ(qclient::describeRedisReply(ans),
    "1) \"message\"\n"
    "2) \"channel\"\n"
    "3) \"payload\"\n");
}

TEST(Formatter, pmessage) {
  qclient::ResponseBuilder builder;
  builder.feed(Formatter::pmessage("pattern", "channel", "payload").val);

  redisReplyPtr ans;
  ASSERT_EQ(builder.pull(ans), qclient::ResponseBuilder::Status::kOk);

  ASSERT_EQ(qclient::describeRedisReply(ans),
    "1) \"pmessage\"\n"
    "2) \"pattern\"\n"
    "3) \"channel\"\n"
    "4) \"payload\"\n");
}
