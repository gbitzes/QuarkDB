// ----------------------------------------------------------------------
// File: test-reply-macros.hh
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

#ifndef __QUARKDB_TEST_REPLY_MACROS_H__
#define __QUARKDB_TEST_REPLY_MACROS_H__

#define ASSERT_REPLY(reply, val) { assert_reply(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }
#define ASSERT_ERR(reply, val) { assert_error(reply, val); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }
#define ASSERT_NIL(reply) { assert_nil(reply); if(::testing::Test::HasFatalFailure()) { FAIL(); return; } }

namespace quarkdb {

void assert_nil(const redisReplyPtr &reply) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_NIL);
}

void assert_nil(std::future<redisReplyPtr> &fut) {
  assert_nil(fut.get());
}

void assert_error(const redisReplyPtr &reply, const std::string &err) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_ERROR);
  ASSERT_EQ(std::string(reply->str, reply->len), err);
}

void assert_reply(const redisReplyPtr &reply, int integer) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_INTEGER);
  ASSERT_EQ(reply->integer, integer);
}

void assert_reply(const redisReplyPtr &reply, const std::string &str) {
  ASSERT_NE(reply, nullptr);
  // ASSERT_TRUE(reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS);
  EXPECT_EQ(std::string(reply->str, reply->len), str);
}

void assert_reply(const redisReplyPtr &reply, const std::vector<std::string> &vec) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, vec.size());

  for(size_t i = 0; i < vec.size(); i++) {
    ASSERT_REPLY(redisReplyPtr(reply->element[i], [](redisReply*){}), vec[i]);
  }
}

void assert_reply(const redisReplyPtr &reply, const std::pair<std::string, std::vector<std::string>> &scan) {
  ASSERT_NE(reply, nullptr);
  ASSERT_EQ(reply->type, REDIS_REPLY_ARRAY);
  ASSERT_EQ(reply->elements, 2u);
  ASSERT_REPLY(redisReplyPtr(reply->element[0], [](redisReply*){}), scan.first);
  ASSERT_REPLY(redisReplyPtr(reply->element[1], [](redisReply*){}), scan.second);
}


// crazy C++ templating to allow ASSERT_REPLY() to work as one liner in all cases
// T&& here is a universal reference
template<typename T>
void assert_reply(std::future<redisReplyPtr> &fut, T&& check) {
  assert_reply(fut.get(), check);
}

template<typename T>
void assert_reply(std::future<redisReplyPtr> &&fut, T&& check) {
  assert_reply(fut.get(), check);
}

}

#endif
