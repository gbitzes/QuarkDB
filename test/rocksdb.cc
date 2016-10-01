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
#include <gtest/gtest.h>

using namespace quarkdb;

#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_NOTFOUND(msg) ASSERT_TRUE(msg.IsNotFound())

class Rocks_DB : public ::testing::Test {
protected:
  virtual void SetUp() {
    store = new RocksDB("/tmp/rocksdb-testdb");
    store->flushall();
  }

  virtual void TearDown() {
    delete store;
  }

  RocksDB *store;
  std::string buffer;
  std::vector<std::string> vec;
  std::vector<std::string> vec2;
};

TEST_F(Rocks_DB, T1) {
  ASSERT_OK(store->set("abc", "cde"));
  ASSERT_OK(store->get("abc", buffer));
  ASSERT_EQ(buffer, "cde");
  ASSERT_OK(store->del("abc"));

  ASSERT_NOTFOUND(store->get("abc", buffer));
  ASSERT_NOTFOUND(store->exists("abc"));
  ASSERT_NOTFOUND(store->del("abc"));

  ASSERT_OK(store->set("123", "345"));
  ASSERT_OK(store->set("qwerty", "asdf"));

  ASSERT_OK(store->keys("*", vec));
  vec2 = {"123", "qwerty"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(store->flushall());
  ASSERT_NOTFOUND(store->exists("123"));
  ASSERT_OK(store->keys("*", vec));
  ASSERT_EQ(vec.size(), 0u);

  int64_t num = 0;

  ASSERT_OK(store->sadd("myset", "qqq", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(store->sismember("myset", "qqq"));
  ASSERT_NOTFOUND(store->sismember("myset", "ppp"));

  num = 0;
  ASSERT_OK(store->sadd("myset", "ppp", num));
  ASSERT_EQ(num, 1);

  num = 0;
  ASSERT_OK(store->sadd("myset", "ppp", num));
  ASSERT_EQ(num, 0);

  ASSERT_OK(store->sismember("myset", "ppp"));
  size_t size;
  ASSERT_OK(store->scard("myset", size));
  ASSERT_EQ(size, 2u);

  ASSERT_OK(store->smembers("myset", vec));
  vec2 = {"ppp", "qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(store->srem("myset", "ppp"));
  ASSERT_NOTFOUND(store->srem("myset", "www"));
  ASSERT_NOTFOUND(store->srem("myset", "ppp"));

  ASSERT_OK(store->scard("myset", size));
  ASSERT_EQ(size, 1u);

  ASSERT_OK(store->smembers("myset", vec));
  vec2 = {"qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_NOTFOUND(store->hget("myhash", "123", buffer));
  ASSERT_OK(store->hset("myhash", "abc", "123"));
  ASSERT_OK(store->hset("myhash", "abc", "234"));
  ASSERT_OK(store->hset("myhash", "abc", "345"));

  ASSERT_OK(store->hlen("myhash", size));
  ASSERT_EQ(size, 1u);

  ASSERT_OK(store->hget("myhash", "abc", buffer));
  ASSERT_EQ(buffer, "345");

  ASSERT_OK(store->hset("myhash", "qqq", "ppp"));
  ASSERT_OK(store->hlen("myhash", size));
  ASSERT_EQ(size, 2u);

  ASSERT_OK(store->hexists("myhash", "qqq"));
  ASSERT_NOTFOUND(store->hexists("myhash", "aaa"));

  ASSERT_OK(store->hkeys("myhash", vec));
  vec2 = {"abc", "qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(store->hvals("myhash", vec));
  vec2 = {"345", "ppp"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(store->hgetall("myhash", vec));
  vec2 = {"abc", "345", "qqq", "ppp"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(store->hincrby("myhash", "val", "1", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(store->hincrby("myhash", "val", "3", num));
  ASSERT_EQ(num, 4);

  ASSERT_OK(store->hincrby("myhash", "val", "-3", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(store->hlen("myhash", size));
  ASSERT_EQ(size, 3u);

  ASSERT_OK(store->hdel("myhash", "val"));
  ASSERT_OK(store->hlen("myhash", size));
  ASSERT_EQ(size, 2u);

  ASSERT_NOTFOUND(store->hexists("myhash", "val"));

}
