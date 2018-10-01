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

#ifndef QUARKDB_COMMANDS_H
#define QUARKDB_COMMANDS_H

#include <map>

namespace quarkdb {

enum class RedisCommand {
  INVALID,

  PING,
  DEBUG,
  MONITOR,
  CLIENT_ID,

  FLUSHALL,

  AUTH,
  HMAC_AUTH_GENERATE_CHALLENGE,
  HMAC_AUTH_VALIDATE_CHALLENGE,

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
  HCLONE,

  LHMSET,
  LHSET,
  LHGET,
  LHLEN,
  LHDEL,
  LHSCAN,

  LHGET_WITH_FALLBACK,
  LHDEL_WITH_FALLBACK,
  LHSET_AND_DEL_FALLBACK,
  CONVERT_HASH_FIELD_TO_LHASH,

  SADD,
  SISMEMBER,
  SREM,
  SMOVE,
  SMEMBERS,
  SCARD,
  SSCAN,

  DEQUE_PUSH_FRONT,
  DEQUE_POP_FRONT,
  DEQUE_PUSH_BACK,
  DEQUE_POP_BACK,
  DEQUE_TRIM_FRONT,
  DEQUE_LEN,
  DEQUE_SCAN_BACK,

  RAW_SCAN,
  RAW_GET_ALL_VERSIONS,

  TYPE,

  EXEC,
  DISCARD,
  MULTI,
  TX_READONLY,
  TX_READWRITE,

  CLOCK_GET,

  LEASE_GET,
  LEASE_ACQUIRE,
  LEASE_RELEASE,

  TIMESTAMPED_LEASE_GET,
  TIMESTAMPED_LEASE_ACQUIRE,
  TIMESTAMPED_LEASE_RELEASE,

  CONFIG_GET,
  CONFIG_SET,
  CONFIG_GETALL,

  RAFT_HANDSHAKE,
  RAFT_APPEND_ENTRIES,
  RAFT_INFO,
  RAFT_LEADER_INFO,
  RAFT_REQUEST_VOTE,
  RAFT_FETCH,
  RAFT_CHECKPOINT,
  RAFT_ATTEMPT_COUP,
  RAFT_ADD_OBSERVER,
  RAFT_REMOVE_MEMBER,
  RAFT_PROMOTE_OBSERVER,
  RAFT_HEARTBEAT,
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
  QUARKDB_MANUAL_COMPACTION,
  QUARKDB_LEVEL_STATS,
  QUARKDB_COMPRESSION_STATS,
  QUARKDB_VERSION,

  RECOVERY_GET,
  RECOVERY_SET,
  RECOVERY_DEL,
  RECOVERY_INFO,

  CONVERT_STRING_TO_INT,
  CONVERT_INT_TO_STRING,
};

enum class CommandType {
  INVALID,

  READ,
  WRITE,
  CONTROL,
  RAFT,
  QUARKDB,
  AUTHENTICATION,
  RECOVERY
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
