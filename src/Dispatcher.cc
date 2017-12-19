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

#include "storage/StagingArea.hh"
#include "utils/CommandParsing.hh"
#include "StateMachine.hh"
#include "Dispatcher.hh"
#include "Utils.hh"
#include "Formatter.hh"

using namespace quarkdb;

RedisDispatcher::RedisDispatcher(StateMachine &rocksdb) : store(rocksdb) {
}

LinkStatus RedisDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  return conn->raw(dispatch(req, 0));
}

RedisEncodedResponse RedisDispatcher::errArgs(RedisRequest &request, LogIndex commit) {
  if(commit > 0) store.noop(commit);
  return Formatter::errArgs(request[0]);
}

RedisEncodedResponse RedisDispatcher::dispatchWrite(StagingArea &stagingArea, RedisRequest &request) {
  qdb_assert(request.getCommandType() == CommandType::WRITE);

  switch(request.getCommand()) {
    case RedisCommand::FLUSHALL: {
      if(request.size() != 1) return Formatter::errArgs(request[0]);
      rocksdb::Status st = store.flushall(stagingArea);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::SET: {
      if(request.size() != 3) return Formatter::errArgs(request[0]);
      rocksdb::Status st = store.set(stagingArea, request[1], request[2]);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::DEL: {
      if(request.size() <= 1) return Formatter::errArgs(request[0]);
      int64_t count = 0;
      rocksdb::Status st = store.del(stagingArea, request.begin()+1, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::HSET: {
      if(request.size() != 4) return Formatter::errArgs(request[0]);

      bool fieldcreated;
      rocksdb::Status st = store.hset(stagingArea, request[1], request[2], request[3], fieldcreated);
      if(!st.ok()) return Formatter::fromStatus(st);

      return Formatter::integer(fieldcreated);
    }
    case RedisCommand::HSETNX: {
      if(request.size() != 4) return Formatter::errArgs(request[0]);

      bool fieldcreated;
      rocksdb::Status st = store.hsetnx(stagingArea, request[1], request[2], request[3], fieldcreated);
      if(!st.ok()) return Formatter::fromStatus(st);

      return Formatter::integer(fieldcreated);
    }
    case RedisCommand::HMSET: {
      if(request.size() <= 3 || request.size() % 2 != 0) return Formatter::errArgs(request[0]);
      rocksdb::Status st = store.hmset(stagingArea, request[1], request.begin()+2, request.end());
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::ok();
    }
    case RedisCommand::HINCRBY: {
      if(request.size() != 4) return Formatter::errArgs(request[0]);
      int64_t ret = 0;
      rocksdb::Status st = store.hincrby(stagingArea, request[1], request[2], request[3], ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(ret);
    }
    case RedisCommand::HINCRBYMULTI: {
      if(request.size() < 4 || ( ((request.size()-1) % 3)) != 0) return Formatter::errArgs(request[0]);
      size_t index = 1;
      int64_t ret = 0;
      while(index < request.size()) {
        int64_t tmpret = 0;
        store.hincrby(stagingArea, request[index], request[index+1], request[index+2], tmpret);
        ret += tmpret;

        index += 3;
      }
      return Formatter::integer(ret);
    }
    case RedisCommand::HINCRBYFLOAT: {
      if(request.size() != 4) return Formatter::errArgs(request[0]);
      double ret = 0;
      rocksdb::Status st = store.hincrbyfloat(stagingArea, request[1], request[2], request[3], ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(std::to_string(ret));
    }
    case RedisCommand::HDEL: {
      if(request.size() <= 2) return Formatter::errArgs(request[0]);
      int64_t count = 0;
      rocksdb::Status st = store.hdel(stagingArea, request[1], request.begin()+2, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SADD: {
      if(request.size() <= 2) return Formatter::errArgs(request[0]);
      int64_t count = 0;
      rocksdb::Status st = store.sadd(stagingArea, request[1], request.begin()+2, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SREM: {
      if(request.size() <= 2) return Formatter::errArgs(request[0]);
      int64_t count = 0;
      rocksdb::Status st = store.srem(stagingArea, request[1], request.begin()+2, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::LPUSH: {
      if(request.size() < 3) return Formatter::errArgs(request[0]);
      int64_t length;
      rocksdb::Status st = store.lpush(stagingArea, request[1], request.begin()+2, request.end(), length);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(length);
    }
    case RedisCommand::RPUSH: {
      if(request.size() < 3) return Formatter::errArgs(request[0]);
      int64_t length;
      rocksdb::Status st = store.rpush(stagingArea, request[1], request.begin()+2, request.end(), length);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(length);
    }
    case RedisCommand::LPOP: {
      if(request.size() != 2) return Formatter::errArgs(request[0]);
      std::string item;
      rocksdb::Status st = store.lpop(stagingArea, request[1], item);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(item);
    }
    case RedisCommand::RPOP: {
      if(request.size() != 2) return Formatter::errArgs(request[0]);
      std::string item;
      rocksdb::Status st = store.rpop(stagingArea, request[1], item);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(item);
    }
    case RedisCommand::CONFIG_SET: {
      if(request.size() != 3) return Formatter::errArgs(request[0]);
      rocksdb::Status st = store.configSet(stagingArea, request[1], request[2]);
      return Formatter::fromStatus(st);
    }
    default: {
      qdb_throw("internal dispatching error in RedisDispatcher for " << request);
    }
  }

}

LinkStatus RedisDispatcher::dispatch(Connection *conn, WriteBatch &batch) {
  StagingArea stagingArea(store);

  LinkStatus lastStatus = 0;
  for(size_t i = 0; i < batch.requests.size(); i++) {
    lastStatus = conn->raw(dispatchWrite(stagingArea, batch.requests[i]));
  }

  stagingArea.commit(0);
  return lastStatus;
}

RedisEncodedResponse RedisDispatcher::dispatch(RedisRequest &request, LogIndex commit) {
  if(request.getCommand() == RedisCommand::INVALID) {
    if(startswith(request[0], "JOURNAL_")) {
      store.noop(commit);
      return Formatter::ok();
    }

    qdb_assert(commit == 0);
    return Formatter::err(SSTR("unknown command " << quotes(request[0])));
  }

  if(commit > 0 && request.getCommandType() != CommandType::WRITE) {
    qdb_throw("attempted to dispatch non-write command '" << request[0] << "' with a positive commit index: " << commit);
  }

  // Handle writes in a separate function, use batch write API
  if(request.getCommandType() == CommandType::WRITE) {
    StagingArea stagingArea(store);
    RedisEncodedResponse response = dispatchWrite(stagingArea, request);
    stagingArea.commit(commit);
    return response;
  }

  switch(request.getCommand()) {
    case RedisCommand::PING: {
      if(request.size() > 2) return errArgs(request, commit);
      if(request.size() == 1) return Formatter::pong();

      return Formatter::string(request[1]);
    }
    case RedisCommand::GET: {
      if(request.size() != 2) return errArgs(request, commit);

      std::string value;
      rocksdb::Status st = store.get(request[1], value);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::EXISTS: {
      if(request.size() <= 1) return errArgs(request, commit);
      int64_t count = 0;
      rocksdb::Status st = store.exists(request.begin()+1, request.end(), count);
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
    case RedisCommand::SCAN: {
      if(request.size() < 2) return errArgs(request, commit);

      ScanCommandArguments args = parseScanCommand(request.begin()+1, request.end());
      if(!args.error.empty()) {
        return Formatter::err(args.error);
      }

      std::string newcursor;
      std::vector<std::string> vec;
      rocksdb::Status st = store.scan(args.cursor, args.match, args.count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
    }
    case RedisCommand::HGET: {
      if(request.size() != 3) return errArgs(request, commit);

      std::string value;
      rocksdb::Status st = store.hget(request[1], request[2], value);
      if(st.IsNotFound()) return Formatter::null();
      else if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
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
      if(request.size() < 3) return errArgs(request, commit);

      ScanCommandArguments args = parseScanCommand(request.begin()+2, request.end());
      if(!args.error.empty()) {
        return Formatter::err(args.error);
      }

      // No support for MATCH here, maybe add later
      if(!args.match.empty()) {
        return Formatter::err("syntax error");
      }

      std::string newcursor;
      std::vector<std::string> vec;
      rocksdb::Status st = store.hscan(request[1], args.cursor, args.count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
    }
    case RedisCommand::SISMEMBER: {
      if(request.size() != 3) return errArgs(request, commit);
      rocksdb::Status st = store.sismember(request[1], request[2]);
      if(st.ok()) return Formatter::integer(1);
      if(st.IsNotFound()) return Formatter::integer(0);
      return Formatter::fromStatus(st);
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
      if(request.size() < 3) return errArgs(request, commit);

      ScanCommandArguments args = parseScanCommand(request.begin()+2, request.end());
      if(!args.error.empty()) {
        return Formatter::err(args.error);
      }

      // No support for MATCH here, maybe add later
      if(!args.match.empty()) {
        return Formatter::err("syntax error");
      }

      std::string newcursor;
      std::vector<std::string> vec;
      rocksdb::Status st = store.sscan(request[1], args.cursor, args.count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
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
    case RedisCommand::CONFIG_GETALL: {
      if(request.size() != 1) return errArgs(request, commit);
      std::vector<std::string> ret;
      rocksdb::Status st = store.configGetall(ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(ret);
    }
    default: {
      std::string msg = SSTR("internal dispatching error for " << quotes(request[0]));
      qdb_critical(msg);
      return Formatter::err(msg);
    }

  }
}
