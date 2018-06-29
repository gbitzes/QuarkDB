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

#include "storage/KeyDescriptor.hh"
#include "storage/StagingArea.hh"
#include "storage/ReverseLocator.hh"
#include "storage/PatternMatching.hh"
#include "storage/ExpirationEventIterator.hh"
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
  ASSERT_OK(stateMachine()->exists(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0);

  elem = {"abc"};
  ASSERT_OK(stateMachine()->del(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0);

  ASSERT_OK(stateMachine()->set("123", "345"));
  ASSERT_OK(stateMachine()->set("qwerty", "asdf"));

  ASSERT_OK(stateMachine()->keys("*", vec));
  vec2 = {"123", "qwerty"};
  ASSERT_EQ(vec, vec2);

  ASSERT_OK(stateMachine()->flushall());

  elem = {"123", "qwerty" };
  ASSERT_OK(stateMachine()->exists(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0);

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
  ASSERT_OK(stateMachine()->verifyChecksum());
}

TEST_F(State_Machine, consistency_check) {
  for(size_t i = 1; i < 10; i++) {
    bool created;
    ASSERT_OK(stateMachine()->hset("hash", SSTR("f" << i), SSTR("v" << i), created));
    ASSERT_TRUE(created);
  }

  ASSERT_OK(stateMachine()->verifyChecksum());
  ASSERT_EQ(ConsistencyScanner::obtainScanPeriod(*stateMachine()), ConsistencyScanner::kDefaultPeriod);
  ASSERT_OK(stateMachine()->configSet(ConsistencyScanner::kConfigurationKey, "1"));
  ASSERT_EQ(ConsistencyScanner::obtainScanPeriod(*stateMachine()), std::chrono::seconds(1));
  ASSERT_OK(stateMachine()->configSet(ConsistencyScanner::kConfigurationKey, "asdf"));
  ASSERT_EQ(ConsistencyScanner::obtainScanPeriod(*stateMachine()), ConsistencyScanner::kDefaultPeriod);
  ASSERT_OK(stateMachine()->configSet(ConsistencyScanner::kConfigurationKey, std::to_string(60 * 60 * 24)));
  ASSERT_EQ(ConsistencyScanner::obtainScanPeriod(*stateMachine()), std::chrono::hours(24));
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

TEST_F(State_Machine, config) {
  LogIndex commitIndex = 0;
  std::string item;

  ASSERT_NOTFOUND(stateMachine()->configGet("raft.resilvering", item));
  ASSERT_OK(stateMachine()->configSet("raft.resilvering", "TRUE", ++commitIndex));
  ASSERT_OK(stateMachine()->configGet("raft.resilvering", item));
  ASSERT_EQ(item, "TRUE");

  ASSERT_OK(stateMachine()->configSet("raft.trimming.step", "123", ++commitIndex));
  ASSERT_OK(stateMachine()->configSet("raft.trimming.limit", "1000", ++commitIndex));

  ASSERT_OK(stateMachine()->configGet("raft.trimming.step", item));
  ASSERT_EQ(item, "123");

  ASSERT_OK(stateMachine()->configGet("raft.trimming.limit", item));
  ASSERT_EQ(item, "1000");

  std::vector<std::string> elem = { "raft.trimming.limit", "raft.trimming.step" };
  int64_t count;
  ASSERT_OK(stateMachine()->exists(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0u);

  ASSERT_OK(stateMachine()->set("raft.trimming.step", "evil", ++commitIndex));
  ASSERT_OK(stateMachine()->configGet("raft.trimming.step", item));
  ASSERT_EQ(item, "123");

  elem = {"raft.trimming.limit"};
  ASSERT_OK(stateMachine()->exists(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0);

  elem = {"raft.trimming.step"};
  ASSERT_OK(stateMachine()->exists(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 1);

  std::vector<std::string> keysToDelete = {"raft.trimming.step"};
  int64_t num = 0;
  ASSERT_OK(stateMachine()->del(keysToDelete.begin(), keysToDelete.end(), num, ++commitIndex));
  ASSERT_EQ(num, 1);
  ASSERT_OK(stateMachine()->configGet("raft.trimming.step", item));
  ASSERT_EQ(item, "123");

  elem = {"raft.trimming.limit"};
  ASSERT_OK(stateMachine()->exists(elem.begin(), elem.end(), count));
  ASSERT_EQ(count, 0);

  ASSERT_OK(stateMachine()->set("random key", "random value", ++commitIndex));
  ASSERT_OK(stateMachine()->set("random key 2", "random value 2", ++commitIndex));

  std::vector<std::string> allkeys;
  ASSERT_OK(stateMachine()->keys("*", allkeys));
  ASSERT_EQ(allkeys, make_vec("random key", "random key 2"));

  ASSERT_OK(stateMachine()->flushall(++commitIndex));
  ASSERT_OK(stateMachine()->keys("*", allkeys));
  ASSERT_EQ(allkeys, make_vec());

  ASSERT_OK(stateMachine()->configGet("raft.trimming.step", item));
  ASSERT_EQ(item, "123");

  std::vector<std::string> contents;
  ASSERT_OK(stateMachine()->configGetall(contents));
  ASSERT_EQ(contents, make_vec("raft.resilvering", "TRUE", "raft.trimming.limit", "1000", "raft.trimming.step", "123"));
}

TEST_F(State_Machine, keys) {
  ASSERT_OK(stateMachine()->set("one", "1"));
  ASSERT_OK(stateMachine()->set("two", "2"));
  ASSERT_OK(stateMachine()->set("three", "3"));
  ASSERT_OK(stateMachine()->set("four", "4"));

  std::vector<std::string> keys;
  ASSERT_OK(stateMachine()->keys("*o*", keys));
  ASSERT_EQ(keys, make_vec("four", "one", "two"));

  ASSERT_OK(stateMachine()->keys("t??", keys));
  ASSERT_EQ(keys, make_vec("two"));

  ASSERT_OK(stateMachine()->keys("*", keys));
  ASSERT_EQ(keys, make_vec("four", "one", "three", "two"));

  ASSERT_OK(stateMachine()->set("hello", "1"));
  ASSERT_OK(stateMachine()->set("hallo", "2"));
  ASSERT_OK(stateMachine()->set("hillo", "3"));
  ASSERT_OK(stateMachine()->set("hllo", "4"));
  ASSERT_OK(stateMachine()->set("heeeello", "5"));

  ASSERT_OK(stateMachine()->keys("h[ae]llo", keys));
  ASSERT_EQ(keys, make_vec("hallo", "hello"));

  ASSERT_OK(stateMachine()->keys("h*llo", keys));
  ASSERT_EQ(keys, make_vec("hallo", "heeeello", "hello", "hillo", "hllo"));

  ASSERT_OK(stateMachine()->keys("h[^e]llo", keys));
  ASSERT_EQ(keys, make_vec("hallo", "hillo"));

  ASSERT_OK(stateMachine()->set("*", "1"));
  ASSERT_OK(stateMachine()->keys("\\*", keys));
  ASSERT_EQ(keys, make_vec("*"));
}

TEST_F(State_Machine, BatchedWrites) {
  StagingArea stagingArea(*stateMachine());

  bool fieldcreated;
  ASSERT_OK(stateMachine()->set(stagingArea, "one", "1"));
  ASSERT_OK(stateMachine()->set(stagingArea, "two", "2"));
  ASSERT_OK(stateMachine()->hset(stagingArea, "key", "field", "value", fieldcreated));
  ASSERT_TRUE(fieldcreated);

  ASSERT_OK(stateMachine()->hset(stagingArea, "key", "field", "value", fieldcreated));
  ASSERT_FALSE(fieldcreated);

  stagingArea.commit(1);

  std::string val;
  ASSERT_OK(stateMachine()->get("one", val));
  ASSERT_EQ(val, "1");

  ASSERT_OK(stateMachine()->get("two", val));
  ASSERT_EQ(val, "2");

  ASSERT_OK(stateMachine()->hget("key", "field", val));
  ASSERT_EQ(val, "value");
}

TEST_F(State_Machine, scan) {
  ASSERT_OK(stateMachine()->set("key1", "1"));
  ASSERT_OK(stateMachine()->set("key2", "2"));
  ASSERT_OK(stateMachine()->set("key3", "3"));
  ASSERT_OK(stateMachine()->set("key4", "4"));
  ASSERT_OK(stateMachine()->set("key5", "4"));
  ASSERT_OK(stateMachine()->set("key6", "4"));
  ASSERT_OK(stateMachine()->set("otherkey1", "5"));
  ASSERT_OK(stateMachine()->set("otherkey2", "6"));
  ASSERT_OK(stateMachine()->set("otherkey3", "7"));
  ASSERT_OK(stateMachine()->set("otherkey4", "8"));

  std::string newcursor;
  std::vector<std::string> keys;
  ASSERT_OK(stateMachine()->scan("", "key*", 2, newcursor, keys));
  ASSERT_EQ(keys, make_vec("key1", "key2"));
  ASSERT_EQ(newcursor, "key3");

  ASSERT_OK(stateMachine()->scan(newcursor, "key*", 2, newcursor, keys));
  ASSERT_EQ(keys, make_vec("key3", "key4"));
  ASSERT_EQ(newcursor, "key5");

  ASSERT_OK(stateMachine()->scan(newcursor, "key*", 2, newcursor, keys));
  ASSERT_EQ(keys, make_vec("key5", "key6"));
  ASSERT_EQ(newcursor, "");

  ASSERT_OK(stateMachine()->scan("", "*key1", 2, newcursor, keys));
  ASSERT_EQ(keys, make_vec("key1"));
  ASSERT_EQ(newcursor, "key3");

  ASSERT_OK(stateMachine()->scan(newcursor, "*key1", 2, newcursor, keys));
  ASSERT_TRUE(keys.empty());
  ASSERT_EQ(newcursor, "key5");

  ASSERT_OK(stateMachine()->scan(newcursor, "*key1", 2, newcursor, keys));
  ASSERT_TRUE(keys.empty());
  ASSERT_EQ(newcursor, "otherkey1");

  ASSERT_OK(stateMachine()->scan(newcursor, "*key1", 2, newcursor, keys));
  ASSERT_EQ(keys, make_vec("otherkey1"));
  ASSERT_EQ(newcursor, "otherkey3");

  ASSERT_OK(stateMachine()->scan(newcursor, "*key1", 2, newcursor, keys));
  ASSERT_TRUE(keys.empty());
  ASSERT_EQ(newcursor, "");

  ASSERT_OK(stateMachine()->set("aba", "6"));
  ASSERT_OK(stateMachine()->set("abb", "7"));
  ASSERT_OK(stateMachine()->set("abc", "8"));
  ASSERT_OK(stateMachine()->set("abcd", "8"));

  ASSERT_OK(stateMachine()->scan("", "ab?", 3, newcursor, keys));
  ASSERT_EQ(keys, make_vec("aba", "abb", "abc"));
  ASSERT_EQ(newcursor, "abcd");

  ASSERT_OK(stateMachine()->scan(newcursor, "ab?", 3, newcursor, keys));
  ASSERT_TRUE(keys.empty());
  ASSERT_EQ(newcursor, "");

  // Using a non-sense cursor
  ASSERT_OK(stateMachine()->scan("zz", "ab?", 100, newcursor, keys));
  ASSERT_TRUE(keys.empty());
  ASSERT_EQ(newcursor, "");

  // Match only a single key
  ASSERT_OK(stateMachine()->scan("", "abc", 100, newcursor, keys));
  ASSERT_EQ(keys, make_vec("abc"));
  ASSERT_EQ(newcursor, "");
}

TEST_F(State_Machine, SnapshotReads) {
  std::unique_ptr<StagingArea> readArea(new StagingArea(*stateMachine(), true));

  std::string tmp;
  ASSERT_NOTFOUND(stateMachine()->get(*readArea, "mykey", tmp));
  ASSERT_OK(stateMachine()->set("mykey", "someval"));

  // readArea still uses the old snapshot, updates to "mykey" should
  // not be visible.
  ASSERT_NOTFOUND(stateMachine()->get(*readArea, "mykey", tmp));

  // Refresh snapshot.
  readArea.reset(new StagingArea(*stateMachine(), true));
  ASSERT_OK(stateMachine()->get(*readArea, "mykey", tmp));
  ASSERT_EQ(tmp, "someval");

  ASSERT_OK(stateMachine()->set("mykey-2", "someval-2"));
  ASSERT_NOTFOUND(stateMachine()->get(*readArea, "mykey-2", tmp));
  ASSERT_OK(stateMachine()->get("mykey-2", tmp));
  ASSERT_EQ(tmp, "someval-2");

  int64_t count = 0;
  std::vector<std::string> vals = {"mykey", "mykey-2"};
  ASSERT_OK(stateMachine()->exists(*readArea, vals.begin(), vals.end(), count));
  ASSERT_EQ(count, 1);
}

TEST_F(State_Machine, Clock) {
  ClockValue clk;
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 0u);

  stateMachine()->advanceClock(ClockValue(123));
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 123u);

  stateMachine()->advanceClock(ClockValue(234));
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 234u);

  ASSERT_THROW(stateMachine()->advanceClock(ClockValue(233)), FatalException);
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 234u);

  stateMachine()->advanceClock(ClockValue(234));
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 234u);

  stateMachine()->advanceClock(ClockValue(345));
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 345u);
}

TEST_F(State_Machine, Leases) {
  ClockValue clk;
  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 0u);

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_FALSE(iterator.valid());
  }


  LeaseInfo info;
  ASSERT_EQ(stateMachine()->lease_acquire("my-lease", "some-string", ClockValue(1), 10, info),
    LeaseAcquisitionStatus::kAcquired);

  ASSERT_EQ(info.getDeadline(), 11u);
  ASSERT_EQ(info.getLastRenewal(), 1u);
  ASSERT_EQ(info.getValue(), "some-string");

  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 1u);

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 11u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease");
    iterator.next();
    ASSERT_FALSE(iterator.valid());
  }

  ASSERT_EQ(stateMachine()->lease_acquire("my-lease", "some-string", ClockValue(9), 10, info),
    LeaseAcquisitionStatus::kRenewed
  );

  ASSERT_EQ(info.getDeadline(), 19u);
  ASSERT_EQ(info.getLastRenewal(), 9u);
  ASSERT_EQ(info.getValue(), "some-string");

  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 9u);

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 19u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease");
    iterator.next();
    ASSERT_FALSE(iterator.valid());
  }

  ASSERT_EQ(stateMachine()->lease_acquire("my-lease", "some-other-string", ClockValue(12), 10, info),
    LeaseAcquisitionStatus::kFailedDueToOtherOwner);

  ASSERT_EQ(info.getDeadline(), 19u);
  ASSERT_EQ(info.getLastRenewal(), 9u);
  ASSERT_EQ(info.getValue(), "some-string");

  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 12u);

  ASSERT_EQ(stateMachine()->lease_acquire("my-lease-2", "some-other-string", ClockValue(13), 10, info),
    LeaseAcquisitionStatus::kAcquired);

  ASSERT_EQ(info.getDeadline(), 23u);
  ASSERT_EQ(info.getLastRenewal(), 13u);
  ASSERT_EQ(info.getValue(), "some-other-string");

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 19u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease");
    iterator.next();
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 23u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease-2");
    iterator.next();
    ASSERT_FALSE(iterator.valid());
  }

  ASSERT_OK(stateMachine()->lease_release("my-lease-2"));
  int64_t count = 0;
  std::vector<std::string> keys = { "my-lease-2" };
  ASSERT_OK(stateMachine()->exists(keys.begin(), keys.end(), count) );
  ASSERT_EQ(count, 0);

  ASSERT_NOTFOUND(stateMachine()->lease_release("not-existing"));

  {
    StagingArea stagingArea(*stateMachine());
    DescriptorLocator locator("my-lease");
    std::string tmp;
    ASSERT_OK(stagingArea.get(locator.toSlice(), tmp));
    KeyDescriptor descr(tmp);
    ASSERT_EQ(descr.getSize(), 11u);
    ASSERT_EQ(descr.getStartIndex(), 9u);
    ASSERT_EQ(descr.getEndIndex(), 19u);
  }

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 19u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease");
    iterator.next();
    ASSERT_FALSE(iterator.valid());
  }

  ASSERT_EQ(stateMachine()->lease_acquire("my-lease-3", "some-other-string", ClockValue(18), 10, info),
    LeaseAcquisitionStatus::kAcquired);

  ASSERT_EQ(info.getDeadline(), 28u);
  ASSERT_EQ(info.getLastRenewal(), 18u);
  ASSERT_EQ(info.getValue(), "some-other-string");

  ASSERT_EQ(stateMachine()->lease_acquire("my-lease-4", "some-other-string", ClockValue(18), 10, info),
    LeaseAcquisitionStatus::kAcquired);

  ASSERT_EQ(info.getDeadline(), 28u);
  ASSERT_EQ(info.getLastRenewal(), 18u);
  ASSERT_EQ(info.getValue(), "some-other-string");

  stateMachine()->getClock(clk);
  ASSERT_EQ(clk, 18u);

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 19u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease");
    iterator.next();
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 28u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease-3");
    iterator.next();
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 28u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease-4");
    iterator.next();
    ASSERT_FALSE(iterator.valid());
  }

  ASSERT_EQ(stateMachine()->lease_acquire("my-lease-4", "some-other-string", ClockValue(25), 10, info),
    LeaseAcquisitionStatus::kRenewed);
  ASSERT_EQ(info.getDeadline(), 35u);
  ASSERT_EQ(info.getLastRenewal(), 25u);
  ASSERT_EQ(info.getValue(), "some-other-string");

  {
    StagingArea stagingArea(*stateMachine());
    ExpirationEventIterator iterator(stagingArea);
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 28u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease-3");
    iterator.next();
    ASSERT_TRUE(iterator.valid());
    ASSERT_EQ(iterator.getDeadline(), 35u);
    ASSERT_EQ(iterator.getRedisKey(), "my-lease-4");
    iterator.next();
    ASSERT_FALSE(iterator.valid());
  }

  ASSERT_OK(stateMachine()->lease_get("my-lease-4", ClockValue(25), info));
  ASSERT_EQ(info.getLastRenewal(), ClockValue(25));
  ASSERT_EQ(info.getDeadline(), ClockValue(35));
  ASSERT_EQ(info.getValue(), "some-other-string");

  ASSERT_NOTFOUND(stateMachine()->lease_get("does-not-exist", ClockValue(25), info));
}

static std::string sliceToString(const rocksdb::Slice &slice) {
  return std::string(slice.data(), slice.size());
}

void assertEqualDescriptors(KeyDescriptor &desc, KeyDescriptor &desc2) {
  ASSERT_EQ(desc.getKeyType(), desc2.getKeyType());
  ASSERT_EQ(desc, desc2);
  ASSERT_EQ(desc2, desc);
  ASSERT_EQ(sliceToString(desc.serialize()), sliceToString(desc2.serialize()));
}

TEST(KeyDescriptor, BasicSanity) {
  KeyDescriptor stringDesc;
  ASSERT_THROW(stringDesc.serialize(), FatalException);

  stringDesc.setKeyType(KeyType::kString);
  stringDesc.setSize(3);
  ASSERT_THROW(stringDesc.setStartIndex(2), FatalException);
  ASSERT_THROW(stringDesc.setStartIndex(4), FatalException);

  ASSERT_EQ(stringDesc, stringDesc);

  KeyDescriptor stringDesc2(sliceToString(stringDesc.serialize()));
  ASSERT_EQ(stringDesc2.getKeyType(), KeyType::kString);
  assertEqualDescriptors(stringDesc, stringDesc2);

  KeyDescriptor hashDesc;
  hashDesc.setKeyType(KeyType::kHash);
  hashDesc.setSize(7);
  ASSERT_THROW(hashDesc.setStartIndex(2), FatalException);
  ASSERT_THROW(hashDesc.setStartIndex(4), FatalException);

  KeyDescriptor hashDesc2(sliceToString(hashDesc.serialize()));
  ASSERT_EQ(hashDesc2.getKeyType(), KeyType::kHash);
  assertEqualDescriptors(hashDesc, hashDesc2);

  ASSERT_FALSE(stringDesc == hashDesc);

  KeyDescriptor listDesc;
  listDesc.setKeyType(KeyType::kList);
  listDesc.setSize(10);
  listDesc.setStartIndex(1500);
  listDesc.setEndIndex(1000);
  ASSERT_THROW(listDesc.serialize(), FatalException);
  listDesc.setEndIndex(1600);

  KeyDescriptor listDesc2(sliceToString(listDesc.serialize()));
  assertEqualDescriptors(listDesc, listDesc2);

  KeyDescriptor setDesc;
  setDesc.setKeyType(KeyType::kSet);
  setDesc.setSize(9);
  ASSERT_THROW(setDesc.setStartIndex(2), FatalException);
  ASSERT_THROW(setDesc.setStartIndex(4), FatalException);

  KeyDescriptor setDesc2(sliceToString(setDesc.serialize()));
  ASSERT_EQ(setDesc2.getKeyType(), KeyType::kSet);
  ASSERT_EQ(setDesc2.getSize(), 9);
  assertEqualDescriptors(setDesc, setDesc2);

  KeyDescriptor leaseDescr;
  leaseDescr.setKeyType(KeyType::kLease);
  leaseDescr.setSize(10);
  leaseDescr.setStartIndex(10);
  leaseDescr.setEndIndex(15);

  KeyDescriptor leaseDescr2(sliceToString(leaseDescr.serialize()));
  ASSERT_EQ(leaseDescr.getKeyType(), KeyType::kLease);
  ASSERT_EQ(leaseDescr.getStartIndex(), 10u);
  ASSERT_EQ(leaseDescr.getEndIndex(), 15u);
}

TEST(FieldLocator, BasicSanity) {
  FieldLocator locator1(KeyType::kHash, "some_key");
  locator1.resetField("my_field");
  ASSERT_EQ(locator1.toSlice().ToString(), SSTR(char(KeyType::kHash) << "some_key##my_field"));

  FieldLocator locator2(KeyType::kSet, "key#with#hashes");
  locator2.resetField("field#with#hashes");
  ASSERT_EQ(locator2.toSlice().ToString(), SSTR(char(KeyType::kSet) << "key|#with|#hashes##field#with#hashes"));
  ASSERT_EQ(locator2.getPrefix(), SSTR(char(KeyType::kSet) << "key|#with|#hashes##"));

  FieldLocator locator3(KeyType::kSet, "evil#key|");
  locator3.resetField("evil#field");
  ASSERT_EQ(locator3.toSlice().ToString(), SSTR(char(KeyType::kSet) << "evil|#key|##evil#field"));
  ASSERT_EQ(locator3.getPrefix(), SSTR(char(KeyType::kSet) << "evil|#key|##"));
}

TEST(ReverseLocator, BasicSanity) {
  FieldLocator locator1(KeyType::kHash, "some_key");
  locator1.resetField("some_field");

  ReverseLocator revlocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kHash);
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "some_key");
  ASSERT_EQ(revlocator.getField().ToString(), "some_field");
  ASSERT_EQ(revlocator.getRawPrefix().ToString(), locator1.getPrefix().ToString());

  const std::string evilkey("evil#key#with|#hashes#|###");
  FieldLocator locator2(KeyType::kSet, evilkey);
  locator2.resetField("field#with#hashes");

  revlocator = ReverseLocator(locator2.toSlice());
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kSet);
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), evilkey);
  ASSERT_EQ(revlocator.getRawPrefix().ToString(), locator2.getPrefix().ToString());
  ASSERT_EQ(revlocator.getField().ToString(), "field#with#hashes");

  StringLocator locator3("random_string###|###");
  revlocator = ReverseLocator(locator3.toSlice());
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kString);
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "random_string###|###");
  ASSERT_THROW(revlocator.getRawPrefix(), FatalException);
  ASSERT_THROW(revlocator.getField(), FatalException);

  revlocator = ReverseLocator("zdfdas");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kParseError);

  revlocator = ReverseLocator(SSTR(char(KeyType::kHash) << "abc#bcd"));
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kParseError);
}

TEST(LocalityFieldLocator, BasicSanity) {
  LocalityFieldLocator locator1("some_key");
  ASSERT_EQ(locator1.toSlice().ToString(), "esome_key##d");

  ReverseLocator revlocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "some_key");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);

  ASSERT_THROW(LocalityFieldLocator(""), FatalException);
  ASSERT_THROW(locator1.resetField("aaa"), FatalException); // need to specify hint first

  locator1.resetHint("my-locality-hint");
  ASSERT_EQ(locator1.toSlice().ToString(), "esome_key##dmy-locality-hint##");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "some_key");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);

  locator1.resetField("field##with##hashes");
  ASSERT_EQ(locator1.toSlice().ToString(), "esome_key##dmy-locality-hint##field##with##hashes");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "some_key");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);

  locator1.resetHint("evil-hint##with##hashes");
  locator1.resetField("a-field");
  ASSERT_EQ(locator1.toSlice().ToString(), "esome_key##devil-hint|#|#with|#|#hashes##a-field");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "some_key");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);

  locator1.resetKey("#evil#key#");
  locator1.resetHint("#evil#hint#");
  locator1.resetField("#evil#field#");
  ASSERT_EQ(locator1.toSlice().ToString(), "e|#evil|#key|###d|#evil|#hint|####evil#field#");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "#evil#key#");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);

  locator1.resetKey("my-key");
  locator1.resetHint("my-hint");
  locator1.resetField("my-field");
  ASSERT_EQ(locator1.toSlice().ToString(), "emy-key##dmy-hint##my-field");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "my-key");
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);
}

TEST(LocalityIndexLocator, BasicSanity) {
  LocalityIndexLocator locator1("my-key", "my-field");
  ASSERT_EQ(locator1.toSlice().ToString(), "emy-key##imy-field");

  ReverseLocator revlocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "my-key");

  locator1.resetKey("key##with##hashes");
  ASSERT_EQ(locator1.toSlice().ToString(), "ekey|#|#with|#|#hashes##i");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "key##with##hashes");

  locator1.resetField("aaaaa");
  ASSERT_EQ(locator1.toSlice().ToString(), "ekey|#|#with|#|#hashes##iaaaaa");
  revlocator = ReverseLocator(locator1.toSlice());
  ASSERT_EQ(revlocator.getKeyType(), KeyType::kLocalityHash);
  ASSERT_EQ(revlocator.getOriginalKey().ToString(), "key##with##hashes");
}

TEST(LeaseLocator, BasicSanity) {
  LeaseLocator locator1("my-key");
  ASSERT_EQ(locator1.toSlice().ToString(), "fmy-key");

  LeaseLocator locator2("my#key");
  ASSERT_EQ(locator2.toSlice().ToString(), "fmy#key");
}

TEST(ExpirationEventLocator, BasicSanity) {
  ExpirationEventLocator locator1(ClockValue(123u), "some-key");
  ASSERT_EQ(locator1.toSlice().ToString(), SSTR("@" << unsignedIntToBinaryString(123u) << "some-key"));
}

TEST(PatternMatching, BasicSanity) {
  ASSERT_EQ(extractPatternPrefix("abc*"), "abc");
  ASSERT_EQ(extractPatternPrefix("abc"), "abc");
  ASSERT_EQ(extractPatternPrefix("ab?abc"), "ab");
  ASSERT_EQ(extractPatternPrefix("1234[a-z]*134"), "1234");
  ASSERT_EQ(extractPatternPrefix("?134"), "");
}
