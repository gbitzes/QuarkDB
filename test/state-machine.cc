// ----------------------------------------------------------------------
// File: state-machine.cc
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

#include "StateMachine.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_NOTFOUND(msg) ASSERT_TRUE(msg.IsNotFound())
#define ASSERT_NOT_OK(msg) ASSERT_FALSE(msg.ok())

class State_Machine : public TestCluster3NodesFixture {};

TEST_F(State_Machine, test_write_transactions) {
  ASSERT_EQ(stateMachine()->getLastApplied(), 0);

  ASSERT_THROW(stateMachine()->set("abc", "123", 2), FatalException);
  ASSERT_OK(stateMachine()->set("abc", "123", 1));
  ASSERT_EQ(stateMachine()->getLastApplied(), 1);
  ASSERT_OK(stateMachine()->set("abc", "122", 2));
  ASSERT_EQ(stateMachine()->getLastApplied(), 2);

  bool created;
  ASSERT_OK(stateMachine()->hset("myhash", "key1", "value", created, 3));
  ASSERT_TRUE(created);
  ASSERT_EQ(stateMachine()->getLastApplied(), 3);

  std::string tmp;
  ASSERT_OK(stateMachine()->hget("myhash", "key1", tmp));
  ASSERT_EQ(tmp, "value");

  std::vector<std::string> elem { "key1", "key2" };
  int64_t count;
  ASSERT_OK(stateMachine()->hdel("myhash", elem.begin(), elem.end(), count, 4));
  ASSERT_EQ(count, 1);
  ASSERT_NOTFOUND(stateMachine()->hget("myhash", "key1", tmp));
  ASSERT_EQ(stateMachine()->getLastApplied(), 4);

  ASSERT_OK(stateMachine()->hdel("myhash", elem.begin(), elem.begin()+1, count, 5));
  ASSERT_EQ(count, 0);
  ASSERT_EQ(stateMachine()->getLastApplied(), 5);

  elem = {"not-existing"};
  ASSERT_OK(stateMachine()->del(elem.begin(), elem.end(), count, 6));
  ASSERT_EQ(count, 0);
  ASSERT_EQ(stateMachine()->getLastApplied(), 6);

  ASSERT_OK(stateMachine()->hset("hash2", "key1", "v2", created, 7));
  ASSERT_TRUE(created);
  ASSERT_EQ(stateMachine()->getLastApplied(), 7);
  ASSERT_NOT_OK(stateMachine()->set("hash2", "wrong type", 8));
  ASSERT_EQ(stateMachine()->getLastApplied(), 8);

  elem = {"hash2", "asdfasdfad"};
  ASSERT_OK(stateMachine()->del(elem.begin(), elem.end(), count, 9));
  ASSERT_EQ(count, 1);
  ASSERT_EQ(stateMachine()->getLastApplied(), 9);

  int64_t added;
  std::vector<std::string> elementsToAdd { "elem1", "elem2" };
  ASSERT_OK(stateMachine()->sadd("set1", elementsToAdd.begin(), elementsToAdd.end(), added, 10));
  ASSERT_EQ(added, 2);
  ASSERT_EQ(stateMachine()->getLastApplied(), 10);

  int64_t removed;
  std::vector<std::string> elementsToRem { "elem2", "elem3" };
  ASSERT_OK(stateMachine()->srem("set1", elementsToRem.begin(), elementsToRem.end(), removed, 11));
  ASSERT_EQ(removed, 1);
  ASSERT_EQ(stateMachine()->getLastApplied(), 11);

  ASSERT_OK(stateMachine()->noop(12));
  ASSERT_EQ(stateMachine()->getLastApplied(), 12);
}

TEST_F(State_Machine, test_hincrby) {
  ASSERT_EQ(stateMachine()->getLastApplied(), 0);

  int64_t result;
  ASSERT_OK(stateMachine()->hincrby("myhash", "counter", "1", result, 1));
  ASSERT_EQ(result, 1);
  ASSERT_EQ(stateMachine()->getLastApplied(), 1);

  ASSERT_NOT_OK(stateMachine()->hincrby("myhash", "counter", "asdf", result, 2));
  ASSERT_EQ(stateMachine()->getLastApplied(), 2);

  ASSERT_OK(stateMachine()->hincrby("myhash", "counter", "5", result, 3));
  ASSERT_EQ(result, 6);
  ASSERT_EQ(stateMachine()->getLastApplied(), 3);

  bool created;
  ASSERT_OK(stateMachine()->hset("myhash", "str", "asdf", created, 4));
  ASSERT_TRUE(created);
  ASSERT_EQ(stateMachine()->getLastApplied(), 4);

  ASSERT_NOT_OK(stateMachine()->hincrby("myhash", "str", "5", result, 5));
  ASSERT_EQ(stateMachine()->getLastApplied(), 5);

  ASSERT_OK(stateMachine()->hincrby("myhash", "counter", "-30", result, 6));
  ASSERT_EQ(stateMachine()->getLastApplied(), 6);
  ASSERT_EQ(result, -24);
}

TEST_F(State_Machine, test_hsetnx) {
  ASSERT_EQ(stateMachine()->getLastApplied(), 0);

  bool created;
  ASSERT_OK(stateMachine()->hsetnx("myhash", "field", "v1", created, 1));
  ASSERT_TRUE(created);
  ASSERT_EQ(stateMachine()->getLastApplied(), 1);

  ASSERT_OK(stateMachine()->hsetnx("myhash", "field", "v2", created, 2));
  ASSERT_FALSE(created);
  ASSERT_EQ(stateMachine()->getLastApplied(), 2);

  std::string value;
  ASSERT_OK(stateMachine()->hget("myhash", "field", value));
  ASSERT_EQ(value, "v1");
}

TEST_F(State_Machine, test_hincrbyfloat) {
  ASSERT_EQ(stateMachine()->getLastApplied(), 0);

  double result;
  ASSERT_OK(stateMachine()->hincrbyfloat("myhash", "field", "0.5", result, 1));
  ASSERT_EQ(stateMachine()->getLastApplied(), 1);
  ASSERT_EQ(result, 0.5);

  std::string tmp;
  ASSERT_OK(stateMachine()->hget("myhash", "field", tmp));
  ASSERT_EQ(tmp, "0.500000");

  ASSERT_OK(stateMachine()->hincrbyfloat("myhash", "field", "0.3", result, 2));
  ASSERT_EQ(stateMachine()->getLastApplied(), 2);

  ASSERT_OK(stateMachine()->hget("myhash", "field", tmp));
  ASSERT_EQ(tmp, "0.800000");
  ASSERT_EQ(result, 0.8);

  bool created;
  ASSERT_OK(stateMachine()->hset("myhash", "field2", "not-a-float", created, 3));
  ASSERT_TRUE(created);
  rocksdb::Status st = stateMachine()->hincrbyfloat("myhash", "field2", "0.1", result, 4);
  ASSERT_EQ(st.ToString(), "Invalid argument: hash value is not a float");
  ASSERT_EQ(stateMachine()->getLastApplied(), 4);
}

TEST_F(State_Machine, basic_sanity) {
  std::string buffer;
  std::vector<std::string> vec, vec2;

  ASSERT_OK(stateMachine()->set("abc", "cde"));
  ASSERT_OK(stateMachine()->get("abc", buffer));
  ASSERT_EQ(buffer, "cde");

  std::vector<std::string> elem = {"abc"};
  int64_t count;
  ASSERT_OK(stateMachine()->del(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 1);

  ASSERT_NOTFOUND(stateMachine()->get("abc", buffer));
  ASSERT_NOTFOUND(stateMachine()->exists("abc"));
  elem = {"abc"};
  ASSERT_OK(stateMachine()->del(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0);

  ASSERT_OK(stateMachine()->set("123", "345"));
  ASSERT_OK(stateMachine()->set("qwerty", "asdf"));

  ASSERT_OK(stateMachine()->keys("*", vec));
  vec2 = {"123", "qwerty"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(stateMachine()->flushall());
  ASSERT_NOTFOUND(stateMachine()->exists("123"));
  ASSERT_OK(stateMachine()->keys("*", vec));
  ASSERT_EQ(vec.size(), 0u);

  int64_t num = 0;
  std::vector<std::string> elements { "qqq" };
  ASSERT_OK(stateMachine()->sadd("myset", elements.begin(), elements.end(), num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(stateMachine()->sismember("myset", "qqq"));
  ASSERT_NOTFOUND(stateMachine()->sismember("myset", "ppp"));

  num = 0;
  elements = { "ppp" };
  ASSERT_OK(stateMachine()->sadd("myset", elements.begin(), elements.end(), num));
  ASSERT_EQ(num, 1);

  num = 0;
  ASSERT_OK(stateMachine()->sadd("myset", elements.begin(), elements.end(), num));
  ASSERT_EQ(num, 0);

  ASSERT_OK(stateMachine()->sismember("myset", "ppp"));
  size_t size;
  ASSERT_OK(stateMachine()->scard("myset", size));
  ASSERT_EQ(size, 2u);

  ASSERT_OK(stateMachine()->smembers("myset", vec));
  vec2 = {"ppp", "qqq"};
  ASSERT_EQ(vec, vec2);

  elements = { "ppp" };
  ASSERT_OK(stateMachine()->srem("myset", elements.begin(), elements.end(), num));
  ASSERT_EQ(num, 1);

  elements = { "www" };
  ASSERT_OK(stateMachine()->srem("myset", elements.begin(), elements.end(), num));
  ASSERT_EQ(num, 0);

  elements = { "ppp" };
  ASSERT_OK(stateMachine()->srem("myset", elements.begin(), elements.end(), num));
  ASSERT_EQ(num, 0);

  ASSERT_OK(stateMachine()->scard("myset", size));
  ASSERT_EQ(size, 1u);

  ASSERT_OK(stateMachine()->smembers("myset", vec));
  vec2 = {"qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_NOTFOUND(stateMachine()->hget("myhash", "123", buffer));
  bool created;
  ASSERT_OK(stateMachine()->hset("myhash", "abc", "123", created));
  ASSERT_TRUE(created);
  ASSERT_OK(stateMachine()->hset("myhash", "abc", "234", created));
  ASSERT_FALSE(created);
  ASSERT_OK(stateMachine()->hset("myhash", "abc", "345", created));
  ASSERT_FALSE(created);

  ASSERT_OK(stateMachine()->hlen("myhash", size));
  ASSERT_EQ(size, 1u);

  ASSERT_OK(stateMachine()->hget("myhash", "abc", buffer));
  ASSERT_EQ(buffer, "345");

  ASSERT_OK(stateMachine()->hset("myhash", "qqq", "ppp", created));
  ASSERT_TRUE(created);
  ASSERT_OK(stateMachine()->hlen("myhash", size));
  ASSERT_EQ(size, 2u);

  ASSERT_OK(stateMachine()->hexists("myhash", "qqq"));
  ASSERT_NOTFOUND(stateMachine()->hexists("myhash", "aaa"));

  ASSERT_OK(stateMachine()->hkeys("myhash", vec));
  vec2 = {"abc", "qqq"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(stateMachine()->hvals("myhash", vec));
  vec2 = {"345", "ppp"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(stateMachine()->hgetall("myhash", vec));
  vec2 = {"abc", "345", "qqq", "ppp"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(stateMachine()->hincrby("myhash", "val", "1", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(stateMachine()->hincrby("myhash", "val", "3", num));
  ASSERT_EQ(num, 4);

  ASSERT_OK(stateMachine()->hincrby("myhash", "val", "-3", num));
  ASSERT_EQ(num, 1);

  ASSERT_OK(stateMachine()->hlen("myhash", size));
  ASSERT_EQ(size, 3u);

  vec2 = { "val" };
  ASSERT_OK(stateMachine()->hdel("myhash", vec2.begin(), vec2.end(), count));
  ASSERT_EQ(count, 1);
  ASSERT_OK(stateMachine()->hlen("myhash", size));
  ASSERT_EQ(size, 2u);

  ASSERT_NOTFOUND(stateMachine()->hexists("myhash", "val"));
}

TEST_F(State_Machine, hscan) {
  std::vector<std::string> vec;
  for(size_t i = 1; i < 10; i++) {
    bool created;
    ASSERT_OK(stateMachine()->hset("hash", SSTR("f" << i), SSTR("v" << i), created));
    ASSERT_TRUE(created);
  }

  std::string newcursor;
  ASSERT_OK(stateMachine()->hscan("hash", "", 3, newcursor, vec));
  ASSERT_EQ(vec, make_vec("f1", "v1", "f2", "v2", "f3", "v3"));
  ASSERT_EQ(newcursor, "f4");

  ASSERT_OK(stateMachine()->hscan("hash", "f4", 4, newcursor, vec));
  ASSERT_EQ(vec, make_vec("f4", "v4", "f5", "v5", "f6", "v6", "f7", "v7"));
  ASSERT_EQ(newcursor, "f8");

  ASSERT_OK(stateMachine()->hscan("hash", "f8", 4, newcursor, vec));
  ASSERT_EQ(vec, make_vec("f8", "v8", "f9", "v9"));
  ASSERT_EQ(newcursor, "");

  ASSERT_OK(stateMachine()->hscan("hash", "zz", 4, newcursor, vec));
  ASSERT_TRUE(vec.empty());
  ASSERT_EQ(newcursor, "");
}

TEST_F(State_Machine, hmset) {
  std::vector<std::string> vec;
  for(size_t i = 1; i <= 3; i++) {
    vec.push_back(SSTR("f" << i));
    vec.push_back(SSTR("v" << i));
  }

  ASSERT_OK(stateMachine()->hmset("hash", vec.begin(), vec.end()));

  for(size_t i = 1; i <= 3; i++) {
    std::string tmp;
    ASSERT_OK(stateMachine()->hget("hash", SSTR("f" << i), tmp));
    ASSERT_EQ(tmp, SSTR("v" << i));
  }

  size_t size;
  ASSERT_OK(stateMachine()->hlen("hash", size));
  ASSERT_EQ(size, 3u);

  ASSERT_THROW(stateMachine()->hmset("hash", vec.begin()+1, vec.end()), FatalException);
}

TEST_F(State_Machine, list_operations) {
  std::vector<std::string> vec = {"item1", "item2", "item3"};
  int64_t length;

  ASSERT_OK(stateMachine()->lpush("my_list", vec.begin(), vec.end(), length));
  ASSERT_EQ(length, 3);

  std::string item;
  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item3");

  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item2");

  vec = { "item4" };
  ASSERT_OK(stateMachine()->lpush("my_list", vec.begin(), vec.end(), length));
  ASSERT_EQ(length, 2);

  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item4");

  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item1");

  ASSERT_NOTFOUND(stateMachine()->lpop("my_list", item));
}

TEST_F(State_Machine, list_operations2) {
  std::vector<std::string> vec = {"item1", "item2", "item3", "item4"};
  int64_t length;

  ASSERT_OK(stateMachine()->rpush("my_list", vec.begin(), vec.end(), length));
  ASSERT_EQ(length, 4);

  size_t len;
  ASSERT_OK(stateMachine()->llen("my_list", len));
  ASSERT_EQ(len, 4u);

  std::string item;
  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item1");

  ASSERT_OK(stateMachine()->llen("my_list", len));
  ASSERT_EQ(len, 3u);

  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item2");

  vec = { "item5" };
  ASSERT_OK(stateMachine()->lpush("my_list", vec.begin(), vec.end(), length));
  ASSERT_EQ(length, 3);

  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item5");

  ASSERT_OK(stateMachine()->llen("my_list", len));
  ASSERT_EQ(len, 2u);

  ASSERT_OK(stateMachine()->rpop("my_list", item));
  ASSERT_EQ(item, "item4");

  ASSERT_OK(stateMachine()->lpop("my_list", item));
  ASSERT_EQ(item, "item3");

  ASSERT_NOTFOUND(stateMachine()->lpop("my_list", item));
  ASSERT_NOTFOUND(stateMachine()->rpop("my_list", item));

  ASSERT_OK(stateMachine()->llen("my_list", len));
  ASSERT_EQ(len, 0u);
}
