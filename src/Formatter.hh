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

#ifndef QUARKDB_FORMATTER_H
#define QUARKDB_FORMATTER_H

#include <rocksdb/status.h>
#include "redis/RedisEncodedResponse.hh"
#include "health/HealthIndicator.hh"
#include "Utils.hh"

namespace quarkdb {

struct RaftEntry;
struct RaftEntryWithIndex;
class RedisRequest;
struct Statistics;

class Formatter {
public:
  //----------------------------------------------------------------------------
  // Composable overloads
  //----------------------------------------------------------------------------
  static void statusVector(std::ostringstream &ss, const std::vector<std::string> &vec);
  static void status(std::ostringstream &ss, std::string_view str);
  static void string(std::ostringstream &ss, std::string_view str);
  static void integer(std::ostringstream &ss, int64_t number);

  //----------------------------------------------------------------------------
  // One-shot overloads
  //----------------------------------------------------------------------------
  static RedisEncodedResponse moved(int64_t shardId, const RaftServer &srv);
  static RedisEncodedResponse err(std::string_view msg);
  static RedisEncodedResponse errArgs(std::string_view cmd);
  static RedisEncodedResponse pong();
  static RedisEncodedResponse string(std::string_view str);
  static RedisEncodedResponse fromStatus(const rocksdb::Status &status);
  static RedisEncodedResponse status(std::string_view str);
  static RedisEncodedResponse ok();
  static RedisEncodedResponse null();
  static RedisEncodedResponse integer(int64_t number);
  static RedisEncodedResponse vector(const std::vector<std::string> &vec);
  static RedisEncodedResponse statusVector(const std::vector<std::string> &vec);
  static RedisEncodedResponse scan(std::string_view marker,  const std::vector<std::string> &vec);
  static RedisEncodedResponse raftEntry(const RaftEntry &entry, bool raw, LogIndex idx = -1);
  static RedisEncodedResponse raftEntries(const std::vector<RaftEntry> &entries, bool raw);
  static RedisEncodedResponse journalScan(LogIndex cursor, const std::vector<RaftEntryWithIndex> &entries);
  static RedisEncodedResponse noauth(std::string_view str);
  static RedisEncodedResponse versionedVector(uint64_t num, const std::vector<std::string> &vec);

  static RedisEncodedResponse subscribe(std::string_view channel, size_t active);
  static RedisEncodedResponse psubscribe(std::string_view pattern, size_t active);
  static RedisEncodedResponse unsubscribe(std::string_view channel, size_t active);
  static RedisEncodedResponse punsubscribe(std::string_view channel, size_t active);
  static RedisEncodedResponse message(std::string_view channel, std::string_view payload);
  static RedisEncodedResponse pmessage(std::string_view pattern, std::string_view channel, std::string_view payload);

  static RedisEncodedResponse simpleRedisRequest(const RedisRequest &req);
  static RedisEncodedResponse redisRequest(const RedisRequest &req);

  static RedisEncodedResponse multiply(const RedisEncodedResponse &resp, size_t factor);
  static RedisEncodedResponse vectorsWithHeaders(const std::vector<std::string> &headers, const std::vector<std::vector<std::string>> &data);
  static RedisEncodedResponse stats(const Statistics &stats);
  static RedisEncodedResponse localHealth(const LocalHealth &lh);

private:
  static RedisEncodedResponse strstrint(std::string_view str1, std::string_view str2, int num);
};

}

#endif
