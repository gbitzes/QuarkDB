// ----------------------------------------------------------------------
// File: rocksdb.cc
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

#include "RocksDB.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_NOTFOUND(msg) ASSERT_TRUE(msg.IsNotFound())
#define ASSERT_NOT_OK(msg) ASSERT_FALSE(msg.ok())

class Rocks_DB : public TestCluster3Nodes {};

TEST_F(Rocks_DB, test_write_transactions) {
  ASSERT_EQ(rocksdb()->getLastApplied(), 0);

  ASSERT_THROW(rocksdb()->set("abc", "123", 2), FatalException);
  ASSERT_OK(rocksdb()->set("abc", "123", 1));
  ASSERT_EQ(rocksdb()->getLastApplied(), 1);
  ASSERT_OK(rocksdb()->set("abc", "122", 2));
  ASSERT_EQ(rocksdb()->getLastApplied(), 2);

  ASSERT_OK(rocksdb()->hset("myhash", "key1", "value", 3));
  ASSERT_EQ(rocksdb()->getLastApplied(), 3);

  std::string tmp;
  ASSERT_OK(rocksdb()->hget("myhash", "key1", tmp));
  ASSERT_EQ(tmp, "value");

  ASSERT_OK(rocksdb()->hdel("myhash", "key1", 4));
  ASSERT_NOTFOUND(rocksdb()->hget("myhash", "key1", tmp));
  ASSERT_EQ(rocksdb()->getLastApplied(), 4);

  ASSERT_NOTFOUND(rocksdb()->hdel("myhash", "key1", 5));
  ASSERT_EQ(rocksdb()->getLastApplied(), 5);

  ASSERT_NOTFOUND(rocksdb()->del("not-existing", 6));
  ASSERT_EQ(rocksdb()->getLastApplied(), 6);

  ASSERT_OK(rocksdb()->hset("hash2", "key1", "v2", 7));
  ASSERT_EQ(rocksdb()->getLastApplied(), 7);

  ASSERT_OK(rocksdb()->del("hash2", 8));
  ASSERT_EQ(rocksdb()->getLastApplied(), 8);

  int64_t added = 0;
  ASSERT_OK(rocksdb()->sadd("set1", "elem1", added, 9));
  ASSERT_EQ(added, 1);
  ASSERT_EQ(rocksdb()->getLastApplied(), 9);

  ASSERT_NOTFOUND(rocksdb()->srem("set1", "elem2", 10));
  ASSERT_EQ(rocksdb()->getLastApplied(), 10);
}

TEST_F(Rocks_DB, test_hincrby) {
  ASSERT_EQ(rocksdb()->getLastApplied(), 0);

  int64_t result;
  ASSERT_OK(rocksdb()->hincrby("myhash", "counter", "1", result, 1));
  ASSERT_EQ(result, 1);
  ASSERT_EQ(rocksdb()->getLastApplied(), 1);

  ASSERT_NOT_OK(rocksdb()->hincrby("myhash", "counter", "asdf", result, 2));
  ASSERT_EQ(rocksdb()->getLastApplied(), 2);

  ASSERT_OK(rocksdb()->hincrby("myhash", "counter", "5", result, 3));
  ASSERT_EQ(result, 6);
  ASSERT_EQ(rocksdb()->getLastApplied(), 3);

  ASSERT_OK(rocksdb()->hset("myhash", "str", "asdf", 4));
  ASSERT_EQ(rocksdb()->getLastApplied(), 4);

  ASSERT_NOT_OK(rocksdb()->hincrby("myhash", "str", "5", result, 5));
  ASSERT_EQ(rocksdb()->getLastApplied(), 5);

  ASSERT_OK(rocksdb()->hincrby("myhash", "counter", "-30", result, 6));
  ASSERT_EQ(rocksdb()->getLastApplied(), 6);
  ASSERT_EQ(result, -24);
}

TEST_F(Rocks_DB, basic_sanity) {
  std::string buffer;
  std::vector<std::string> vec, vec2;

  ASSERT_OK(rocksdb()->set("abc", "cde"));
  ASSERT_OK(rocksdb()->get("abc", buffer));
  ASSERT_EQ(buffer, "cde");
  ASSERT_OK(rocksdb()->del("abc"));

  ASSERT_NOTFOUND(rocksdb()->get("abc", buffer));
  ASSERT_NOTFOUND(rocksdb()->exists("abc"));
  ASSERT_NOTFOUND(rocksdb()->del("abc"));

  ASSERT_OK(rocksdb()->set("123", "345"));
  ASSERT_OK(rocksdb()->set("qwerty", "asdf"));

  ASSERT_OK(rocksdb()->keys("*", vec));
  vec2 = {"123", "qwerty"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(rocksdb()->flushall());
  ASSERT_NOTFOUND(rocksdb()->exists("123"));
  ASSERT_OK(rocksdb()->keys("*", vec));
  ASSERT_EQ(vec.size(), 0u);

  int64_t num = 0;

  ASSERT_OK(rocksdb()->sadd("myset", "qqq", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(rocksdb()->sismember("myset", "qqq"));
  ASSERT_NOTFOUND(rocksdb()->sismember("myset", "ppp"));

  num = 0;
  ASSERT_OK(rocksdb()->sadd("myset", "ppp", num));
  ASSERT_EQ(num, 1);

  num = 0;
  ASSERT_OK(rocksdb()->sadd("myset", "ppp", num));
  ASSERT_EQ(num, 0);

  ASSERT_OK(rocksdb()->sismember("myset", "ppp"));
  size_t size;
  ASSERT_OK(rocksdb()->scard("myset", size));
  ASSERT_EQ(size, 2u);

  ASSERT_OK(rocksdb()->smembers("myset", vec));
  vec2 = {"ppp", "qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(rocksdb()->srem("myset", "ppp"));
  ASSERT_NOTFOUND(rocksdb()->srem("myset", "www"));
  ASSERT_NOTFOUND(rocksdb()->srem("myset", "ppp"));

  ASSERT_OK(rocksdb()->scard("myset", size));
  ASSERT_EQ(size, 1u);

  ASSERT_OK(rocksdb()->smembers("myset", vec));
  vec2 = {"qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_NOTFOUND(rocksdb()->hget("myhash", "123", buffer));
  ASSERT_OK(rocksdb()->hset("myhash", "abc", "123"));
  ASSERT_OK(rocksdb()->hset("myhash", "abc", "234"));
  ASSERT_OK(rocksdb()->hset("myhash", "abc", "345"));

  ASSERT_OK(rocksdb()->hlen("myhash", size));
  ASSERT_EQ(size, 1u);

  ASSERT_OK(rocksdb()->hget("myhash", "abc", buffer));
  ASSERT_EQ(buffer, "345");

  ASSERT_OK(rocksdb()->hset("myhash", "qqq", "ppp"));
  ASSERT_OK(rocksdb()->hlen("myhash", size));
  ASSERT_EQ(size, 2u);

  ASSERT_OK(rocksdb()->hexists("myhash", "qqq"));
  ASSERT_NOTFOUND(rocksdb()->hexists("myhash", "aaa"));

  ASSERT_OK(rocksdb()->hkeys("myhash", vec));
  vec2 = {"abc", "qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(rocksdb()->hvals("myhash", vec));
  vec2 = {"345", "ppp"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(rocksdb()->hgetall("myhash", vec));
  vec2 = {"abc", "345", "qqq", "ppp"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(rocksdb()->hincrby("myhash", "val", "1", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(rocksdb()->hincrby("myhash", "val", "3", num));
  ASSERT_EQ(num, 4);

  ASSERT_OK(rocksdb()->hincrby("myhash", "val", "-3", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(rocksdb()->hlen("myhash", size));
  ASSERT_EQ(size, 3u);

  ASSERT_OK(rocksdb()->hdel("myhash", "val"));
  ASSERT_OK(rocksdb()->hlen("myhash", size));
  ASSERT_EQ(size, 2u);

  ASSERT_NOTFOUND(rocksdb()->hexists("myhash", "val"));
}
