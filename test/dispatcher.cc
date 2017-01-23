// ----------------------------------------------------------------------
// File: dispatcher.cc
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

#include "Dispatcher.hh"
#include "BufferedReader.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

#define ASSERT_OK(msg) ASSERT_TRUE(msg.ok())
#define ASSERT_NOTFOUND(msg) ASSERT_TRUE(msg.IsNotFound())

class Dispatcher_ : public ::testing::Test {
protected:
  Dispatcher_()
  : store("/tmp/rocksdb-testdb"),
    conn(&link),
    bufferManager(NULL, NULL),
    reader(&link, &bufferManager),
    dispatcher(store) {

    store.flushall();
  }

  void assert_reply(RedisRequest &&request, const std::string &reply) {
    EXPECT_EQ(dispatcher.dispatch(&conn, request), (int) reply.size());
    std::string tmp;
    ASSERT_EQ(reader.consume(reply.size(), tmp), (int) reply.size());
    ASSERT_EQ(tmp, reply);
  }

  StateMachine store;
  Link link;
  Connection conn;
  XrdBuffManager bufferManager;
  BufferedReader reader;
  RedisDispatcher dispatcher;
  std::string buffer;
};


TEST_F(Dispatcher_, T1) {
  assert_reply( {"wrOng CoMmand"}, "-ERR unknown command 'wrOng CoMmand'\r\n");
  assert_reply( {"PINGGG"}, "-ERR unknown command 'PINGGG'\r\n");
  assert_reply( {"PIN"}, "-ERR unknown command 'PIN'\r\n");
  assert_reply( {"ping"}, "+PONG\r\n" );
  assert_reply( {"Ping"}, "+PONG\r\n" );
  assert_reply( {"PiNg"}, "+PONG\r\n" );
  assert_reply( {"PING"}, "+PONG\r\n" );
  assert_reply( {"flushall"}, "+OK\r\n" );
  assert_reply( {"Fflushall"}, "-ERR unknown command 'Fflushall'\r\n" );

  assert_reply( {"set", "abc", "12345"}, "+OK\r\n");
  ASSERT_OK(store.get("abc", buffer));
  ASSERT_EQ(buffer, "12345");

  assert_reply( {"set", "qqq", "ppp"}, "+OK\r\n");
  ASSERT_OK(store.get("qqq", buffer));
  ASSERT_EQ(buffer, "ppp");

  assert_reply( {"get", "abc"}, "$5\r\n12345\r\n");
  assert_reply( {"get", "notexists"}, "$-1\r\n");

  assert_reply( {"exists", "notexists"}, ":0\r\n");
  assert_reply( {"exists", "notexists", "abc"}, ":1\r\n");
  assert_reply( {"exists", "abc"}, ":1\r\n");
  assert_reply( {"exists", "abc", "qqq"}, ":2\r\n");
  assert_reply( {"exists", "abc", "notexists", "qqq"}, ":2\r\n");

  assert_reply( {"del", "notexists"}, ":0\r\n");
  assert_reply( {"del", "abc", "qqq"}, ":2\r\n");
  assert_reply( {"exists", "abc", "qqq"}, ":0\r\n");

  assert_reply( {"keys", "*" },  "*0\r\n");
  assert_reply( {"set", "abc", "12345"}, "+OK\r\n");
  assert_reply( {"set", "qqq", "ppp"}, "+OK\r\n");

  assert_reply( {"keys", "*" },  "*2\r\n$3\r\nabc\r\n$3\r\nqqq\r\n");
  assert_reply( {"keys", "a*" },  "*1\r\n$3\r\nabc\r\n");
  assert_reply( {"keys", "q*" },  "*1\r\n$3\r\nqqq\r\n");

  assert_reply( {"hset", "myhash", "abc", "123"}, ":1\r\n");
  assert_reply( {"hset", "myhash", "abc", "12345"}, ":0\r\n");

  assert_reply( {"hget", "myhash", "abc" }, "$5\r\n12345\r\n");
  assert_reply( {"keys", "myh*" },  "*1\r\n$6\r\nmyhash\r\n");
  assert_reply( {"hget", "myhash", "abc", "cde"}, "-ERR wrong number of arguments for 'hget' command\r\n");
  assert_reply( {"hexists", "myhash", "abc" }, ":1\r\n");
  assert_reply( {"hexists", "myhash", "notexist" }, ":0\r\n");

  assert_reply( {"hset", "myhash", "key2", "54321"}, ":1\r\n");
  assert_reply( {"hkeys", "myhash" }, "*2\r\n$3\r\nabc\r\n$4\r\nkey2\r\n");
  assert_reply( {"hkeys", "notexists" }, "*0\r\n");

  assert_reply( {"hgetall", "myhash"}, "*4\r\n$3\r\nabc\r\n$5\r\n12345\r\n$4\r\nkey2\r\n$5\r\n54321\r\n");
  assert_reply( {"hvals", "myhash"}, "*2\r\n$5\r\n12345\r\n$5\r\n54321\r\n");
  assert_reply( {"hscan", "myhash", "0"}, "*2\r\n$1\r\n0\r\n"
                "*4\r\n$3\r\nabc\r\n$5\r\n12345\r\n$4\r\nkey2\r\n$5\r\n54321\r\n");

  assert_reply( {"hincrby", "myhash", "counter", "1"}, ":1\r\n");
  assert_reply( {"hincrby", "myhash", "counter", "2"}, ":3\r\n");
  assert_reply( {"hincrby", "myhash", "counter", "-3"}, ":0\r\n");
  assert_reply( {"hlen", "myhash"}, ":3\r\n");

  assert_reply( {"hdel", "myhash", "counter", "key2"}, ":2\r\n");
  assert_reply( {"hvals", "myhash"}, "*1\r\n$5\r\n12345\r\n");
  assert_reply( {"hlen", "myhash"}, ":1\r\n");
  assert_reply( {"hdel", "myhash", "counter", "key2", "abc"}, ":1\r\n");
  assert_reply( {"hlen", "myhash"}, ":0\r\n");
  assert_reply( {"hscan", "myhash", "0"}, "*2\r\n$1\r\n0\r\n*0\r\n");

  assert_reply( {"sadd", "myset", "a", "b", "c", "d" }, ":4\r\n");
  assert_reply( {"sadd", "myset", "a", "b", "c", "d" }, ":0\r\n");
  assert_reply( {"sadd", "myset", "b", "c", "d", "e", "f" }, ":2\r\n");

  assert_reply( {"sismember", "myset", "a"}, ":1\r\n");
  assert_reply( {"sismember", "myset", "e"}, ":1\r\n");
  assert_reply( {"sismember", "myset", "g"}, ":0\r\n");

  assert_reply( {"smembers", "myset"}, "*6\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n");
  assert_reply( {"sscan", "myset", "0"}, "*2\r\n$1\r\n0\r\n*6\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n");
  assert_reply( {"scard", "myset"}, ":6\r\n");
  assert_reply( {"scard", "asdf"}, ":0\r\n");

  assert_reply( {"srem", "myset", "a", "b"}, ":2\r\n");
  assert_reply( {"smembers", "myset"}, "*4\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n");
  assert_reply( {"srem", "myset", "a", "b"}, ":0\r\n");
  assert_reply( {"srem", "myset", "a", "b", "c"}, ":1\r\n");
  assert_reply( {"sismember", "myset", "a"}, ":0\r\n");
  assert_reply( {"smembers", "myset"}, "*3\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n");
  assert_reply( {"sscan", "myset", "0"}, "*2\r\n$1\r\n0\r\n*3\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n");
  assert_reply( {"smembers", "asdf"}, "*0\r\n");
  assert_reply( {"scard", "myset"}, ":3\r\n");
  assert_reply( {"sscan", "asdf", "0"}, "*2\r\n$1\r\n0\r\n*0\r\n");

}
