//-----------------------------------------------------------------------
// File: Commands.hh
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

#ifndef __QUARKDB_COMMANDS_H__
#define __QUARKDB_COMMANDS_H__

#include <map>

namespace quarkdb {

enum class RedisCommand {
  PING,
  DEBUG,

  FLUSHALL,

  GET,
  SET,
  EXISTS,
  DEL,
  KEYS,

  HGET,
  HSET,
  HEXISTS,
  HKEYS,
  HGETALL,
  HINCRBY,
  HDEL,
  HLEN,
  HVALS,
  HSCAN,

  SADD,
  SISMEMBER,
  SREM,
  SMEMBERS,
  SCARD,
  SSCAN,

  RAFT_HANDSHAKE,
  RAFT_APPEND_ENTRIES,
  RAFT_INFO,
  RAFT_REQUEST_VOTE,
  RAFT_PANIC,
  RAFT_FETCH
};

enum class CommandType {
  READ,
  WRITE,
  CONTROL,
  RAFT
};

struct caseInsensitiveComparator {
    bool operator() (const std::string& lhs, const std::string& rhs) const {
        for(size_t i = 0; i < std::min(lhs.size(), rhs.size()); i++) {
          if(tolower(lhs[i]) != tolower(rhs[i])) {
            return tolower(lhs[i] < tolower(rhs[i]));
          }
        }
        return lhs.size() < rhs.size();
    }
};

extern std::map<std::string,
                std::pair<RedisCommand, CommandType>,
                caseInsensitiveComparator>
                redis_cmd_map;
}

#endif
