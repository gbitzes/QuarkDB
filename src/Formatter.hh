//-----------------------------------------------------------------------
// File: Formatter.hh
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

#ifndef __QUARKDB_FORMATTER_H__
#define __QUARKDB_FORMATTER_H__

#include <rocksdb/status.h>
#include "redis/RedisEncodedResponse.hh"
#include "Utils.hh"

namespace quarkdb {

struct RaftEntry;
class RedisRequest;

class Formatter {
public:
  static RedisEncodedResponse moved(int64_t shardId, const RaftServer &srv);
  static RedisEncodedResponse err(const std::string &msg);
  static RedisEncodedResponse errArgs(const std::string &cmd);
  static RedisEncodedResponse pong();
  static RedisEncodedResponse string(const std::string &str);
  static RedisEncodedResponse fromStatus(const rocksdb::Status &status);
  static RedisEncodedResponse status(const std::string &str);
  static RedisEncodedResponse ok();
  static RedisEncodedResponse null();
  static RedisEncodedResponse integer(int64_t number);
  static RedisEncodedResponse vector(const std::vector<std::string> &vec);
  static RedisEncodedResponse statusVector(const std::vector<std::string> &vec);
  static RedisEncodedResponse scan(const std::string &marker, const std::vector<std::string> &vec);
  static RedisEncodedResponse raftEntry(const RaftEntry &entry, bool raw);
  static RedisEncodedResponse raftEntries(const std::vector<RaftEntry> &entries, bool raw);
  static RedisEncodedResponse noauth(const std::string &str);

  static RedisEncodedResponse simpleRedisRequest(const RedisRequest &req);
  static RedisEncodedResponse redisRequest(const RedisRequest &req);
};

}

#endif
