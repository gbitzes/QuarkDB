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
  INVALID,

  PING,
  DEBUG,
  MONITOR,
  CLIENT_ID,

  FLUSHALL,

  GET,
  SET,
  EXISTS,
  DEL,
  KEYS,
  SCAN,

  HGET,
  HSET,
  HMSET,
  HEXISTS,
  HKEYS,
  HGETALL,
  HINCRBY,
  HINCRBYMULTI,
  HDEL,
  HLEN,
  HVALS,
  HSCAN,
  HSETNX,
  HINCRBYFLOAT,

  SADD,
  SISMEMBER,
  SREM,
  SMEMBERS,
  SCARD,
  SSCAN,

  LPUSH,
  LPOP,
  RPUSH,
  RPOP,
  LLEN,

  MULTI,
  MULTIOP_READ,
  MULTIOP_READWRITE,

  CONFIG_GET,
  CONFIG_SET,
  CONFIG_GETALL,

  RAFT_HANDSHAKE,
  RAFT_APPEND_ENTRIES,
  RAFT_INFO,
  RAFT_REQUEST_VOTE,
  RAFT_FETCH,
  RAFT_CHECKPOINT,
  RAFT_ATTEMPT_COUP,
  RAFT_ADD_OBSERVER,
  RAFT_REMOVE_MEMBER,
  RAFT_PROMOTE_OBSERVER,
  RAFT_HEARTBEAT,
  RAFT_INVALID_COMMAND,                     // used in tests
  RAFT_FETCH_LAST,
  ACTIVATE_STALE_READS,

  QUARKDB_INFO,
  QUARKDB_DETACH,
  QUARKDB_ATTACH,
  QUARKDB_START_RESILVERING,
  QUARKDB_FINISH_RESILVERING,
  QUARKDB_RESILVERING_COPY_FILE,
  QUARKDB_CANCEL_RESILVERING,
  QUARKDB_BULKLOAD_FINALIZE,
  QUARKDB_INVALID_COMMAND,                  // used in tests
  RECOVERY_INFO
};

enum class CommandType {
  INVALID,

  READ,
  WRITE,
  MULTIOP,
  CONTROL,
  RAFT,
  QUARKDB
};

#define QDB_ALWAYS_INLINE __attribute__((always_inline))

struct CommandComparator {

    QDB_ALWAYS_INLINE
    char normalize(char c) const {
      char ret = tolower(c);
      if(ret == '-') {
        ret = '_';
      }
      return ret;
    }

    bool operator() (const std::string& lhs, const std::string& rhs) const {
        for(size_t i = 0; i < std::min(lhs.size(), rhs.size()); i++) {
          char left = normalize(lhs[i]);
          char right = normalize(rhs[i]);

          if(left != right) {
            return left < right;
          }
        }
        return lhs.size() < rhs.size();
    }
};

extern std::map<std::string,
                std::pair<RedisCommand, CommandType>,
                CommandComparator>
                redis_cmd_map;
}

#endif
