//-----------------------------------------------------------------------
// File: Formatter.cc
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

#include "RedisRequest.hh"
#include "raft/RaftCommon.hh"
#include "Common.hh"
#include "Formatter.hh"
#include "redis/ArrayResponseBuilder.hh"
#include "redis/MultiOp.hh"
using namespace quarkdb;

RedisEncodedResponse Formatter::moved(int64_t shardId, const RaftServer &location) {
  return RedisEncodedResponse(SSTR("-MOVED " << shardId << " " << location.toString() << "\r\n"));
}

RedisEncodedResponse Formatter::err(const std::string &err) {
  return RedisEncodedResponse(SSTR("-ERR " << err << "\r\n"));
}

RedisEncodedResponse Formatter::errArgs(const std::string &cmd) {
  qdb_warn("Received malformed " << quotes(cmd) << " command - wrong number of arguments");
  return RedisEncodedResponse(SSTR("-ERR wrong number of arguments for '" << cmd << "' command\r\n"));
}

RedisEncodedResponse Formatter::pong() {
  return RedisEncodedResponse(SSTR("+PONG\r\n"));
}

RedisEncodedResponse Formatter::string(const std::string &str) {
  return RedisEncodedResponse(SSTR("$" << str.length() << "\r\n" << str << "\r\n"));
}

RedisEncodedResponse Formatter::status(const std::string &str) {
  return RedisEncodedResponse(SSTR("+" << str << "\r\n"));
}

RedisEncodedResponse Formatter::ok() {
  return RedisEncodedResponse("+OK\r\n");
}

RedisEncodedResponse Formatter::null() {
  return RedisEncodedResponse("$-1\r\n");
}

RedisEncodedResponse Formatter::integer(int64_t number) {
  return RedisEncodedResponse(SSTR(":" << number << "\r\n"));
}

RedisEncodedResponse Formatter::fromStatus(const rocksdb::Status &status) {
  if(status.ok()) return Formatter::ok();
  return Formatter::err(status.ToString());
}

RedisEncodedResponse Formatter::vector(const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return RedisEncodedResponse(ss.str());
}

RedisEncodedResponse Formatter::statusVector(const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "+" << *it << "\r\n";
  }
  return RedisEncodedResponse(ss.str());
}

RedisEncodedResponse Formatter::scan(const std::string &marker, const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*2\r\n";
  ss << "$" << marker.length() << "\r\n";
  ss << marker << "\r\n";

  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return RedisEncodedResponse(ss.str());
}

RedisEncodedResponse Formatter::simpleRedisRequest(const RedisRequest &req) {
  std::vector<std::string> vec;

  for(size_t i = 0; i < req.size(); i++) {
    vec.emplace_back(req[i]);
  }

  return Formatter::vector(vec);
}

RedisEncodedResponse Formatter::redisRequest(const RedisRequest &req) {
  if(req.getCommand() == RedisCommand::MULTIOP_READWRITE || req.getCommand() == RedisCommand::MULTIOP_READ) {
    MultiOp multiOp;
    multiOp.deserialize(req[1]);

    ArrayResponseBuilder builder(multiOp.size() + 1);
    builder.push_back(Formatter::string(req[0]));

    for(size_t i = 0; i < multiOp.size(); i++) {
      builder.push_back(simpleRedisRequest(multiOp[i]));
    }

    return builder.buildResponse();
  }

  // Simple case, no transactions.
  return simpleRedisRequest(req);
}

RedisEncodedResponse Formatter::raftEntry(const RaftEntry &entry, bool raw) {
  // Very inefficient with copying, but this function is only to help
  // debugging, so we don't really mind.

  ArrayResponseBuilder builder(2);

  builder.push_back(Formatter::string(SSTR(entry.term)));

  if(raw) {
    builder.push_back(simpleRedisRequest(entry.request));
  }
  else {
    builder.push_back(redisRequest(entry.request));
  }

  return builder.buildResponse();
}

RedisEncodedResponse Formatter::raftEntries(const std::vector<RaftEntry> &entries, bool raw) {
  std::stringstream ss;
  ss << "*" << entries.size() << "\r\n";

  for(size_t i = 0; i < entries.size(); i++) {
    ss << Formatter::raftEntry(entries[i], raw).val;
  }

  return RedisEncodedResponse(ss.str());
}

RedisEncodedResponse Formatter::noauth(const std::string &str) {
  return RedisEncodedResponse(SSTR("-NOAUTH " << str << "\r\n"));
}

RedisEncodedResponse Formatter::multiply(const RedisEncodedResponse &resp, size_t factor) {
  qdb_assert(factor >= 1);

  std::ostringstream ss;
  for(size_t i = 0; i < factor; i++) {
    ss << resp.val;
  }

  return RedisEncodedResponse(ss.str());
}
