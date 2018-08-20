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
#include "utils/ParseUtils.hh"
#include "redis/Transaction.hh"
#include "redis/ArrayResponseBuilder.hh"
#include "StateMachine.hh"
#include "Dispatcher.hh"
#include "Utils.hh"
#include "Formatter.hh"

using namespace quarkdb;

RedisEncodedResponse Dispatcher::handleConversion(RedisRequest &request) {
  // Provide simple commands for conversion between binary-string-encoded
  // integers and human-readable ASCII representation.
  //
  // This is strictly for interactive use, to simplify life when debugging and
  // dealing with binary-string integers, especially when using the recovery
  // tool, or inspecting the contents of the raft journal.
  //
  // Please don't use this in scripts.. Using QDB as "binary-string conversion"
  // service would be really silly.

  switch(request.getCommand()) {
    case RedisCommand::CONVERT_STRING_TO_INT: {
      if(request.size() != 2u) return Formatter::errArgs(request[0]);
      if(request[1].size() != 8u) return Formatter::err(SSTR("expected string with 8 characters, was given " << request[1].size() << " instead"));

      std::vector<std::string> reply;
      reply.emplace_back(SSTR("Interpreted as int64_t: " << binaryStringToInt(request[1].c_str())));
      reply.emplace_back(SSTR("Interpreted as uint64_t: " << binaryStringToUnsignedInt(request[1].c_str())));

      return Formatter::statusVector(reply);
    }
    case RedisCommand::CONVERT_INT_TO_STRING: {
      if(request.size() != 2u) return Formatter::errArgs(request[0]);

      int64_t value;
      if(!ParseUtils::parseInt64(request[1], value)) {
        return Formatter::err("cannot parse integer");
      }

      std::vector<std::string> reply;
      reply.emplace_back(SSTR("As int64_t: " << intToBinaryString(value)));
      reply.emplace_back(SSTR("As uint64_t: " << unsignedIntToBinaryString(value)));
      return Formatter::vector(reply);
    }
    default: {
      qdb_throw("internal dispatching error for " << request.toPrintableString());
    }
  }
}

RedisEncodedResponse Dispatcher::handlePing(RedisRequest &request) {
  qdb_assert(request.getCommand() == RedisCommand::PING);

  if(request.size() > 2) return Formatter::errArgs(request[0]);
  if(request.size() == 1) return Formatter::pong();
  return Formatter::string(request[1]);
}

RedisDispatcher::RedisDispatcher(StateMachine &rocksdb) : store(rocksdb) {
}

LinkStatus RedisDispatcher::dispatch(Connection *conn, Transaction &transaction) {
  return conn->raw(dispatch(transaction, 0));
}

LinkStatus RedisDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  return conn->raw(dispatch(req, 0));
}

RedisEncodedResponse RedisDispatcher::errArgs(RedisRequest &request) {
  return Formatter::errArgs(request[0]);
}

RedisEncodedResponse RedisDispatcher::dispatchingError(RedisRequest &request, LogIndex commit) {
  std::string msg = SSTR("internal dispatching error for " << quotes(request[0]));
  qdb_critical(msg);
  if(commit != 0) qdb_throw("Could not dispatch request " << quotes(request[0]) << " with positive commit index: " << commit);
  return Formatter::err(msg);
}

RedisEncodedResponse RedisDispatcher::dispatchReadOnly(StagingArea &stagingArea, Transaction &transaction) {
  qdb_assert(!transaction.containsWrites());
  ArrayResponseBuilder builder(transaction.size(), transaction.isPhantom());

  for(size_t i = 0; i < transaction.size(); i++) {
    builder.push_back(dispatchRead(stagingArea, transaction[i]));
  }

  return builder.buildResponse();
}

RedisEncodedResponse RedisDispatcher::dispatch(StagingArea &stagingArea, Transaction &transaction) {
  ArrayResponseBuilder builder(transaction.size(), transaction.isPhantom());

  for(size_t i = 0; i < transaction.size(); i++) {
    builder.push_back(dispatchReadWrite(stagingArea, transaction[i]));
  }

  return builder.buildResponse();
}

RedisEncodedResponse RedisDispatcher::dispatch(Transaction &transaction, LogIndex commit) {
  StagingArea stagingArea(store, !transaction.containsWrites());

  RedisEncodedResponse resp = dispatch(stagingArea, transaction);

  if(transaction.containsWrites()) {
    stagingArea.commit(commit);
  }

  store.getRequestCounter().account(transaction);
  return resp;
}

RedisEncodedResponse RedisDispatcher::dispatchLHSET(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &hint, const std::string &value) {
  bool fieldcreated;
  rocksdb::Status st = store.lhset(stagingArea, key, field, hint, value, fieldcreated);
  if(!st.ok()) return Formatter::fromStatus(st);
  return Formatter::integer(fieldcreated);
}

RedisEncodedResponse RedisDispatcher::dispatchHDEL(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end) {
  int64_t count = 0;
  rocksdb::Status st = store.hdel(stagingArea, key, start, end, count);
  if(!st.ok()) return Formatter::fromStatus(st);
  return Formatter::integer(count);
}

RedisEncodedResponse RedisDispatcher::dispatchLHDEL(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end) {
  int64_t count = 0;
  rocksdb::Status st = store.lhdel(stagingArea, key, start, end, count);
  if(!st.ok()) return Formatter::fromStatus(st);
  return Formatter::integer(count);
}

RedisEncodedResponse RedisDispatcher::dispatchWrite(StagingArea &stagingArea, RedisRequest &request) {
  qdb_assert(request.getCommandType() == CommandType::WRITE);

  switch(request.getCommand()) {
    case RedisCommand::FLUSHALL: {
      if(request.size() != 1) return errArgs(request);
      rocksdb::Status st = store.flushall(stagingArea);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::SET: {
      if(request.size() != 3) return errArgs(request);
      rocksdb::Status st = store.set(stagingArea, request[1], request[2]);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::DEL: {
      if(request.size() <= 1) return errArgs(request);
      int64_t count = 0;
      rocksdb::Status st = store.del(stagingArea, request.begin()+1, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::HSET: {
      if(request.size() != 4) return errArgs(request);

      bool fieldcreated;
      rocksdb::Status st = store.hset(stagingArea, request[1], request[2], request[3], fieldcreated);
      if(!st.ok()) return Formatter::fromStatus(st);

      return Formatter::integer(fieldcreated);
    }
    case RedisCommand::HSETNX: {
      if(request.size() != 4) return errArgs(request);

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
      if(request.size() != 4) return errArgs(request);
      int64_t ret = 0;
      rocksdb::Status st = store.hincrby(stagingArea, request[1], request[2], request[3], ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(ret);
    }
    case RedisCommand::HINCRBYMULTI: {
      if(request.size() < 4 || ( ((request.size()-1) % 3)) != 0) return errArgs(request);
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
      if(request.size() != 4) return errArgs(request);
      double ret = 0;
      rocksdb::Status st = store.hincrbyfloat(stagingArea, request[1], request[2], request[3], ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(std::to_string(ret));
    }
    case RedisCommand::HDEL: {
      if(request.size() <= 2) return errArgs(request);
      return dispatchHDEL(stagingArea, request[1], request.begin()+2, request.end());
    }
    case RedisCommand::HCLONE: {
      if(request.size() != 3) return errArgs(request);

      rocksdb::Status st = store.hclone(stagingArea, request[1], request[2]);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::ok();
    }
    case RedisCommand::SADD: {
      if(request.size() <= 2) return errArgs(request);
      int64_t count = 0;
      rocksdb::Status st = store.sadd(stagingArea, request[1], request.begin()+2, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SREM: {
      if(request.size() <= 2) return errArgs(request);
      int64_t count = 0;
      rocksdb::Status st = store.srem(stagingArea, request[1], request.begin()+2, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SMOVE: {
      if(request.size() != 4) return errArgs(request);
      int64_t count = 0;
      rocksdb::Status st = store.smove(stagingArea, request[1], request[2], request[3], count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::LPUSH: {
      if(request.size() < 3) return errArgs(request);
      int64_t length;
      rocksdb::Status st = store.dequePushFront(stagingArea, request[1], request.begin()+2, request.end(), length);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(length);
    }
    case RedisCommand::RPUSH: {
      if(request.size() < 3) return errArgs(request);
      int64_t length;
      rocksdb::Status st = store.dequePushBack(stagingArea, request[1], request.begin()+2, request.end(), length);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(length);
    }
    case RedisCommand::LPOP: {
      if(request.size() != 2) return errArgs(request);
      std::string item;
      rocksdb::Status st = store.dequePopFront(stagingArea, request[1], item);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(item);
    }
    case RedisCommand::RPOP: {
      if(request.size() != 2) return errArgs(request);
      std::string item;
      rocksdb::Status st = store.dequePopBack(stagingArea, request[1], item);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(item);
    }
    case RedisCommand::CONFIG_SET: {
      if(request.size() != 3) return errArgs(request);
      rocksdb::Status st = store.configSet(stagingArea, request[1], request[2]);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::LHSET: {
      if(request.size() != 5) return errArgs(request);
      return dispatchLHSET(stagingArea, request[1], request[2], request[3], request[4]);
    }
    case RedisCommand::LHDEL: {
      if(request.size() <= 2) return errArgs(request);
      return dispatchLHDEL(stagingArea, request[1], request.begin()+2, request.end());
    }
    case RedisCommand::LHMSET: {
      if(request.size() <= 4 || (request.size()-2) % 3 != 0) return Formatter::errArgs(request[0]);
      rocksdb::Status st = store.lhmset(stagingArea, request[1], request.begin()+2, request.end());
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::ok();
    }
    case RedisCommand::LHSET_AND_DEL_FALLBACK: {
      if(request.size() != 6) return errArgs(request);
      RedisEncodedResponse resp = dispatchLHSET(stagingArea, request[1], request[2], request[3], request[4]);
      dispatchHDEL(stagingArea, request[5], request.begin()+2, request.begin()+3);
      return resp;
    }
    case RedisCommand::LHDEL_WITH_FALLBACK: {
      if(request.size() != 4) return errArgs(request);
      RedisEncodedResponse resp = dispatchLHDEL(stagingArea, request[1], request.begin()+2, request.begin()+3);
      if(resp.val != Formatter::integer(1).val) {
        return dispatchHDEL(stagingArea, request[3], request.begin()+2, request.begin()+3);
      }

      return resp;
    }
    case RedisCommand::CONVERT_HASH_FIELD_TO_LHASH: {
      if(request.size() != 6) return errArgs(request);

      std::string shouldBeEmpty;
      rocksdb::Status st = store.lhget(stagingArea, request[3], request[4], request[5], shouldBeEmpty);
      if(st.ok()) return Formatter::err("Destination field already exists!");

      std::string value;
      st = store.hget(stagingArea, request[1], request[2], value);
      if(!st.ok()) return Formatter::fromStatus(st);

      bool fieldCreated = false;
      st = store.lhset(stagingArea, request[3], request[4], request[5], value, fieldCreated);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(!fieldCreated) qdb_throw("should never happen");

      int64_t removed = 0;
      st = store.hdel(stagingArea, request[1], request.begin()+2, request.begin()+3, removed);
      if(!st.ok()) qdb_throw("should never happen");
      qdb_assert(removed == 1);

      return Formatter::ok();
    }
    case RedisCommand::TIMESTAMPED_LEASE_ACQUIRE: {
      if(request.size() != 5) return Formatter::errArgs("lease_acquire");

      int64_t duration = 0;
      if(!my_strtoll(request[3], duration) || duration < 1) {
        return Formatter::err("value is not an integer or out of range");
      }

      qdb_assert(request[4].size() == 8u);

      ClockValue timestamp = binaryStringToUnsignedInt(request[4].c_str());
      LeaseInfo leaseInfo;
      LeaseAcquisitionStatus status = store.lease_acquire(stagingArea, request[1], request[2], timestamp, duration, leaseInfo);

      if(status == LeaseAcquisitionStatus::kKeyTypeMismatch) {
        return Formatter::err("Invalid Argument: WRONGTYPE Operation against a key holding the wrong kind of value");
      }
      else if(status == LeaseAcquisitionStatus::kAcquired) {
        return Formatter::status("ACQUIRED");
      }
      else if(status == LeaseAcquisitionStatus::kRenewed) {
        return Formatter::status("RENEWED");
      }
      else {
        qdb_assert(status == LeaseAcquisitionStatus::kFailedDueToOtherOwner);
        return Formatter::err(SSTR("lease held by '" << leaseInfo.getValue() << "', time remaining " << leaseInfo.getDeadline() - timestamp << " ms"));
      }
    }
    case RedisCommand::TIMESTAMPED_LEASE_GET: {
      if(request.size() != 3) return Formatter::errArgs("lease_get");

      qdb_assert(request[2].size() == 8u);
      ClockValue timestamp = binaryStringToUnsignedInt(request[2].c_str());

      LeaseInfo leaseInfo;
      rocksdb::Status st = store.lease_get(stagingArea, request[1], timestamp, leaseInfo);

      if(st.IsNotFound()) {
        return Formatter::null();
      }

      qdb_assert(st.ok());

      std::vector<std::string> reply;
      reply.emplace_back(SSTR("HOLDER: " << leaseInfo.getValue()));
      reply.emplace_back(SSTR("REMAINING: " << leaseInfo.getDeadline() - timestamp << " ms"));
      return Formatter::statusVector(reply);
    }
    case RedisCommand::TIMESTAMPED_LEASE_RELEASE: {
      if(request.size() != 3) return Formatter::errArgs("lease_release");

      qdb_assert(request[2].size() == 8u);
      ClockValue timestamp = binaryStringToUnsignedInt(request[2].c_str());

      rocksdb::Status st = store.lease_release(stagingArea, request[1], timestamp);

      if(st.IsNotFound()) {
        return Formatter::null();
      }

      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::ok();
    }
    case RedisCommand::TX_READWRITE: {
      // Unpack transaction and process
      Transaction transaction;
      qdb_assert(transaction.deserialize(request));
      return dispatch(stagingArea, transaction);
    }
    default: {
      qdb_throw("internal dispatching error in RedisDispatcher for " << request);
    }
  }

}

RedisEncodedResponse RedisDispatcher::dispatchHGET(StagingArea &stagingArea, const std::string &key, const std::string &field) {
  std::string value;
  rocksdb::Status st = store.hget(stagingArea, key, field, value);
  if(st.IsNotFound()) return Formatter::null();
  else if(!st.ok()) return Formatter::fromStatus(st);
  return Formatter::string(value);
}

RedisEncodedResponse RedisDispatcher::dispatchLHGET(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &hint) {
  std::string value;
  rocksdb::Status st = store.lhget(stagingArea, key, field, hint, value);
  if(st.IsNotFound()) return Formatter::null();
  else if(!st.ok()) return Formatter::fromStatus(st);
  return Formatter::string(value);
}

RedisEncodedResponse RedisDispatcher::dispatchRead(StagingArea &stagingArea, RedisRequest &request) {
  switch(request.getCommand()) {
    case RedisCommand::GET: {
      if(request.size() != 2) return errArgs(request);

      std::string value;
      rocksdb::Status st = store.get(stagingArea, request[1], value);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::EXISTS: {
      if(request.size() <= 1) return errArgs(request);
      int64_t count = 0;
      rocksdb::Status st = store.exists(stagingArea, request.begin()+1, request.end(), count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::KEYS: {
      if(request.size() != 2) return errArgs(request);
      std::vector<std::string> ret;
      rocksdb::Status st = store.keys(stagingArea, request[1], ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(ret);
    }
    case RedisCommand::SCAN: {
      if(request.size() < 2) return errArgs(request);

      ScanCommandArguments args = parseScanCommand(request.begin()+1, request.end());
      if(!args.error.empty()) {
        return Formatter::err(args.error);
      }

      std::string newcursor;
      std::vector<std::string> vec;
      rocksdb::Status st = store.scan(stagingArea, args.cursor, args.match, args.count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
    }
    case RedisCommand::HGET: {
      if(request.size() != 3) return errArgs(request);
      return dispatchHGET(stagingArea, request[1], request[2]);
    }
    case RedisCommand::HEXISTS: {
      if(request.size() != 3) return errArgs(request);
      rocksdb::Status st = store.hexists(stagingArea, request[1], request[2]);
      if(st.ok()) return Formatter::integer(1);
      if(st.IsNotFound()) return Formatter::integer(0);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::HKEYS: {
      if(request.size() != 2) return errArgs(request);
      std::vector<std::string> keys;
      rocksdb::Status st = store.hkeys(stagingArea, request[1], keys);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(keys);
    }
    case RedisCommand::HGETALL: {
      if(request.size() != 2) return errArgs(request);
      std::vector<std::string> vec;
      rocksdb::Status st = store.hgetall(stagingArea, request[1], vec);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(vec);
    }
    case RedisCommand::HLEN: {
      if(request.size() != 2) return errArgs(request);
      size_t len;
      rocksdb::Status st = store.hlen(stagingArea, request[1], len);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(len);
    }
    case RedisCommand::HVALS: {
      if(request.size() != 2) return errArgs(request);
      std::vector<std::string> values;
      rocksdb::Status st = store.hvals(stagingArea, request[1], values);
      return Formatter::vector(values);
    }
    case RedisCommand::HSCAN: {
      if(request.size() < 3) return errArgs(request);

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
      rocksdb::Status st = store.hscan(stagingArea, request[1], args.cursor, args.count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
    }
    case RedisCommand::SISMEMBER: {
      if(request.size() != 3) return errArgs(request);
      rocksdb::Status st = store.sismember(stagingArea, request[1], request[2]);
      if(st.ok()) return Formatter::integer(1);
      if(st.IsNotFound()) return Formatter::integer(0);
      return Formatter::fromStatus(st);
    }
    case RedisCommand::SMEMBERS: {
      if(request.size() != 2) return errArgs(request);
      std::vector<std::string> members;
      rocksdb::Status st = store.smembers(stagingArea, request[1], members);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(members);
    }
    case RedisCommand::SCARD: {
      if(request.size() != 2) return errArgs(request);
      size_t count;
      rocksdb::Status st = store.scard(stagingArea, request[1], count);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(count);
    }
    case RedisCommand::SSCAN: {
      if(request.size() < 3) return errArgs(request);

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
      rocksdb::Status st = store.sscan(stagingArea, request[1], args.cursor, args.count, newcursor, vec);
      if(!st.ok()) return Formatter::fromStatus(st);

      if(newcursor == "") newcursor = "0";
      else newcursor = "next:" + newcursor;
      return Formatter::scan(newcursor, vec);
    }
    case RedisCommand::LLEN: {
      if(request.size() != 2) return errArgs(request);
      size_t len;
      rocksdb::Status st = store.dequeLen(stagingArea, request[1], len);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(len);
    }
    case RedisCommand::CONFIG_GET: {
      if(request.size() != 2) return errArgs(request);

      std::string value;
      rocksdb::Status st = store.configGet(stagingArea, request[1], value);
      if(st.IsNotFound()) return Formatter::null();
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::CONFIG_GETALL: {
      if(request.size() != 1) return errArgs(request);
      std::vector<std::string> ret;
      rocksdb::Status st = store.configGetall(stagingArea, ret);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(ret);
    }
    case RedisCommand::LHGET: {
      if(request.size() == 3) {
        return dispatchLHGET(stagingArea, request[1], request[2], "");
      }

      if(request.size() == 4) {
        return dispatchLHGET(stagingArea, request[1], request[2], request[3]);
      }

      return errArgs(request);
    }
    case RedisCommand::LHGET_WITH_FALLBACK: {
      // First, try LHGET...
      RedisEncodedResponse resp;
      if(request.size() == 4) {
        resp = dispatchLHGET(stagingArea, request[1], request[2], "");
      }
      else if(request.size() == 5) {
        resp = dispatchLHGET(stagingArea, request[1], request[2], request[3]);
      }

      // Did we succeed?
      if(resp.val != Formatter::null().val) return resp;

      // Nope, fallback. Look for the same field, but in the hash specified as
      // last argument.
      return dispatchHGET(stagingArea, request[request.size()-1], request[2]);
    }
    case RedisCommand::LHLEN: {
      if(request.size() != 2) return errArgs(request);
      size_t len;
      rocksdb::Status st = store.lhlen(stagingArea, request[1], len);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::integer(len);
    }
    case RedisCommand::RAW_SCAN: {
      if(request.size() != 2 && request.size() != 4) return errArgs(request);

      rocksdb::Status st;
      std::vector<std::string> results;
      if(request.size() == 2) {
        st = store.rawScan(stagingArea, request[1], 50, results);
      }
      else {
        if(!caseInsensitiveEquals(request[2], "count")) {
          return Formatter::err("syntax error");
        }

        int64_t count;
        if(!ParseUtils::parseInt64(request[3], count) || count <= 0) {
          return Formatter::err("syntax error");
        }

        st = store.rawScan(stagingArea, request[1], count, results);
      }

      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::vector(results);
    }
    case RedisCommand::RAW_GET_ALL_VERSIONS: {
      if(request.size() != 2) return errArgs(request);

      std::vector<rocksdb::KeyVersion> versions;
      rocksdb::Status st = store.rawGetAllVersions(request[1], versions);
      if(!st.ok()) return Formatter::fromStatus(st);

      std::vector<std::string> reply;
      for(const rocksdb::KeyVersion& ver : versions) {
        reply.emplace_back(SSTR("KEY: " << ver.user_key));
        reply.emplace_back(SSTR("VALUE: " << ver.value));
        reply.emplace_back(SSTR("SEQUENCE: " << ver.sequence));
        reply.emplace_back(SSTR("TYPE: " << ver.type));
      }

      return Formatter::vector(reply);
    }
    case RedisCommand::CLOCK_GET: {
      if(request.size() != 1) return errArgs(request);

      std::vector<std::string> reply;

      ClockValue staticClock;
      store.getClock(stagingArea, staticClock);

      ClockValue dynamicClock = store.getDynamicClock();

      reply.emplace_back(SSTR("STATIC-CLOCK: " << staticClock));
      reply.emplace_back(SSTR("DYNAMIC-CLOCK: " << dynamicClock));

      return Formatter::statusVector(reply);
    }
    case RedisCommand::TX_READONLY: {
      // Unpack transaction and process
      Transaction transaction;
      qdb_assert(transaction.deserialize(request));
      return dispatchReadOnly(stagingArea, transaction);
    }
    default: {
      return dispatchingError(request, 0);
    }
  }
}

RedisEncodedResponse RedisDispatcher::dispatch(RedisRequest &request, LogIndex commit) {
  if(request.getCommand() == RedisCommand::INVALID) {
    if(startswith(request[0], "JOURNAL_")) {

      if(request[0] == "JOURNAL_LEADERSHIP_MARKER") {
        // Hard-synchronize our dynamic clock to the static one. The dynamic
        // clock is only used in leaders to timestamp incoming lease requests.
        // So, strictly speaking, synchronizing the clock is only necessary for
        // leader nodes, but it's so cheap to do that we don't care. Let's
        // synchronize all nodes.
        store.hardSynchronizeDynamicClock();
      }

      store.noop(commit);
      return Formatter::ok();
    }

    qdb_assert(commit == 0);
    return Formatter::err(SSTR("unknown command " << quotes(request[0])));
  }

  if(commit > 0 && request.getCommandType() != CommandType::WRITE) {
    qdb_throw("attempted to dispatch non-write command '" << request[0] << "' with a positive commit index: " << commit);
  }

  if(request.getCommand() == RedisCommand::PING) {
    return handlePing(request);
  }

  if(request.getCommandType() != CommandType::READ && request.getCommandType() != CommandType::WRITE) {
    return dispatchingError(request, commit);
  }

  return dispatchReadWriteAndCommit(request, commit);
}

RedisEncodedResponse RedisDispatcher::dispatchReadWrite(StagingArea &stagingArea, RedisRequest &request) {
  if(request.getCommandType() == CommandType::WRITE) {
    return dispatchWrite(stagingArea, request);
  }

  return dispatchRead(stagingArea, request);
}

RedisEncodedResponse RedisDispatcher::dispatchReadWriteAndCommit(RedisRequest &request, LogIndex commit) {
  StagingArea stagingArea(store, request.getCommandType() == CommandType::READ);

  RedisEncodedResponse response = dispatchReadWrite(stagingArea, request);

  // Handle writes in a separate function, use batch write API
  if(request.getCommandType() == CommandType::WRITE) {
    stagingArea.commit(commit);
  }

  store.getRequestCounter().account(request);
  return response;
}
