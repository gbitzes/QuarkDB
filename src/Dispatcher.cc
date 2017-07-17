//-----------------------------------------------------------------------
// File: Dispatcher.cc
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
#include "Utils.hh"
#include "Formatter.hh"

using namespace quarkdb;

RedisDispatcher::RedisDispatcher(StateMachine &rocksdb) : store(rocksdb) {
}

LinkStatus RedisDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  return conn->raw(dispatch(req, 0));
}

std::string RedisDispatcher::errArgs(RedisRequest &request, LogIndex commit) {
  if(commit > 0) store.noop(commit);
  return Formatter::errArgs(request[0]);
}

std::string RedisDispatcher::dispatch(RedisRequest &req, LogIndex commit) {
  auto it = redis_cmd_map.find(req[0]);
  if(it != redis_cmd_map.end()) {
    if(commit > 0 && it->second.second != CommandType::WRITE) {
      qdb_throw("attempted to dispatch non-write command '" << req[0] << "' with a positive commit index: " << commit);
    }

    return dispatch(req, it->second.first, commit);
  }

  if(startswith(req[0], "JOURNAL_")) {
    store.noop(commit);
  }

  return Formatter::err(SSTR("unknown command " << quotes(req[0])));
}

std::string RedisDispatcher::dispatch(RedisRequest &request, RedisCommand cmd, LogIndex commit) {
  switch(cmd) {
    case RedisCommand::PING: {
      if(request.size() > 2) return errArgs(request, commit);
      if(request.size() == 1) return Formatter::pong();

      return Formatter::string(request[1]);
    }
    case RedisCommand::FLUSHALL: {
      if(request.size() != 1) return errArgs(request, commit);
      rocksdb::Status st = store.flushall(commit);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::GET: {
      if(request.size() != 2) return errArgs(request, commit);

      std::string value;
      rocksdb::Status st = store.get(request[1], value);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::SET: {
      if(request.size() != 3) return errArgs(request, commit);
      rocksdb::Status st = store.set(request[1], request[2], commit);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::EXISTS: {
      if(request.size() <= 1) return errArgs(request, commit);
      int64_t count = 0;
      for(size_t i = 1; i < request.size(); i++) {
        rocksdb::Status st = store.exists(request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return Formatter::fromStatus(st);
      }
      return Formatter::integer(count);
    }
    case RedisCommand::DEL: {
      if(request.size() <= 1) return errArgs(request, commit);
      int64_t count = 0;
      rocksdb::Status st = store.del(request.begin()+1, request.end(), count, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::KEYS: {
      if(request.size() != 2) return errArgs(request, commit);
      std::vector<std::string> ret;
      rocksdb::Status st = store.keys(request[1], ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(ret);
    }
    case RedisCommand::HGET: {
      if(request.size() != 3) return errArgs(request, commit);

      std::string value;
      rocksdb::Status st = store.hget(request[1], request[2], value);
      if(st.IsNotFound()) return Formatter::null();
      else if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::HSET: {
      if(request.size() != 4) return errArgs(request, commit);

      bool fieldcreated;
      rocksdb::Status st = store.hset(request[1], request[2], request[3], fieldcreated, commit);
      if(!st.ok()) return Formatter::fromStatus(st);

      return Formatter::integer(fieldcreated);
    }
    case RedisCommand::HSETNX: {
      if(request.size() != 4) return errArgs(request, commit);

      bool fieldcreated;
      rocksdb::Status st = store.hsetnx(request[1], request[2], request[3], fieldcreated, commit);
      if(!st.ok()) return Formatter::fromStatus(st);

      return Formatter::integer(fieldcreated);
    }
    case RedisCommand::HMSET: {
      if(request.size() <= 3 || request.size() % 2 != 0) return errArgs(request, commit);
      rocksdb::Status st = store.hmset(request[1], request.begin()+2, request.end(), commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::ok();
    }
    case RedisCommand::HEXISTS: {
      if(request.size() != 3) return errArgs(request, commit);
      rocksdb::Status st = store.hexists(request[1], request[2]);
      if(st.ok()) return Formatter::integer(1);
      if(st.IsNotFound()) return Formatter::integer(0);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::HKEYS: {
      if(request.size() != 2) return errArgs(request, commit);
      std::vector<std::string> keys;
      rocksdb::Status st = store.hkeys(request[1], keys);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(keys);
    }
    case RedisCommand::HGETALL: {
      if(request.size() != 2) return errArgs(request, commit);
      std::vector<std::string> vec;
      rocksdb::Status st = store.hgetall(request[1], vec);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(vec);
    }
    case RedisCommand::HINCRBY: {
      if(request.size() != 4) return errArgs(request, commit);
      int64_t ret = 0;
      rocksdb::Status st = store.hincrby(request[1], request[2], request[3], ret, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(ret);
    }
    case RedisCommand::HINCRBYFLOAT: {
      if(request.size() != 4) return errArgs(request, commit);
      double ret = 0;
      rocksdb::Status st = store.hincrbyfloat(request[1], request[2], request[3], ret, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(std::to_string(ret));
    }
    case RedisCommand::HDEL: {
      if(request.size() <= 2) return errArgs(request, commit);
      int64_t count = 0;
      rocksdb::Status st = store.hdel(request[1], request.begin()+2, request.end(), count, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::HLEN: {
      if(request.size() != 2) return errArgs(request, commit);
      size_t len;
      rocksdb::Status st = store.hlen(request[1], len);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(len);
    }
    case RedisCommand::HVALS: {
      if(request.size() != 2) return errArgs(request, commit);
      std::vector<std::string> values;
      rocksdb::Status st = store.hvals(request[1], values);
      return Formatter::vector(values);
    }
    case RedisCommand::HSCAN: {
      if(request.size() != 3 && request.size() != 5) return errArgs(request, commit);
      std::string cursor;
      int64_t count = 100;

      if(request[2] == "0") {
        cursor = "";
      }
      else if(startswith(request[2], "next:")) {
        cursor = std::string(request[2].begin() + 5, request[2].end());
      }
      else {
        return Formatter::err("invalid cursor");
      }

      if(request.size() == 5) {
        if(!caseInsensitiveEquals(request[3], "count")) return Formatter::err("syntax error");
        if(startswith(request[4], "-") || request[4] == "0") return Formatter::err("syntax error");
        if(!my_strtoll(request[4], count)) return Formatter::err("value is not an integer or out of range");
      }

      std::string newcursor;
      std::vector<std::string> vec;
      rocksdb::Status st = store.hscan(request[1], cursor, count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
    }
    case RedisCommand::SADD: {
      if(request.size() <= 2) return errArgs(request, commit);
      int64_t count = 0;
      rocksdb::Status st = store.sadd(request[1], request.begin()+2, request.end(), count, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SISMEMBER: {
      if(request.size() != 3) return errArgs(request, commit);
      rocksdb::Status st = store.sismember(request[1], request[2]);
      if(st.ok()) return Formatter::integer(1);
      if(st.IsNotFound()) return Formatter::integer(0);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::SREM: {
      if(request.size() <= 2) return errArgs(request, commit);
      int64_t count = 0;
      rocksdb::Status st = store.srem(request[1], request.begin()+2, request.end(), count, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SMEMBERS: {
      if(request.size() != 2) return errArgs(request, commit);
      std::vector<std::string> members;
      rocksdb::Status st = store.smembers(request[1], members);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(members);
    }
    case RedisCommand::SCARD: {
      if(request.size() != 2) return errArgs(request, commit);
      size_t count;
      rocksdb::Status st = store.scard(request[1], count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SSCAN: {
      if(request.size() != 3) return errArgs(request, commit);
      if(request[2] != "0") return Formatter::err("invalid cursor");
      std::vector<std::string> members;
      rocksdb::Status st = store.smembers(request[1], members);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::scan("0", members);
    }
    case RedisCommand::LPUSH: {
      if(request.size() < 3) return errArgs(request, commit);
      int64_t length;
      rocksdb::Status st = store.lpush(request[1], request.begin()+2, request.end(), length, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(length);
    }
    case RedisCommand::RPUSH: {
      if(request.size() < 3) return errArgs(request, commit);
      int64_t length;
      rocksdb::Status st = store.rpush(request[1], request.begin()+2, request.end(), length, commit);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(length);
    }
    case RedisCommand::LPOP: {
      if(request.size() != 2) return errArgs(request, commit);
      std::string item;
      rocksdb::Status st = store.lpop(request[1], item, commit);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(item);
    }
    case RedisCommand::RPOP: {
      if(request.size() != 2) return errArgs(request, commit);
      std::string item;
      rocksdb::Status st = store.rpop(request[1], item, commit);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(item);
    }
    case RedisCommand::LLEN: {
      if(request.size() != 2) return errArgs(request, commit);
      size_t len;
      rocksdb::Status st = store.llen(request[1], len);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(len);
    }
    case RedisCommand::CONFIG_GET: {
      if(request.size() != 2) return errArgs(request, commit);

      std::string value;
      rocksdb::Status st = store.configGet(request[1], value);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::CONFIG_SET: {
      if(request.size() != 3) return errArgs(request, commit);
      rocksdb::Status st = store.configSet(request[1], request[2], commit);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::CONFIG_GETALL: {
      if(request.size() != 1) return errArgs(request, commit);
      std::vector<std::string> ret;
      rocksdb::Status st = store.configGetall(ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(ret);
    }
    default: {
      std::string msg = SSTR("internal dispatching error for " << quotes(request[0]) << " - raft not enabled?");
      qdb_critical(msg);
      return Formatter::err(msg);
    }

  }
}
