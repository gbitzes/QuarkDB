// ----------------------------------------------------------------------
// File: Commands.cc
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

#include "Commands.hh"
using namespace quarkdb;

std::map<std::string,
         std::pair<RedisCommand, CommandType>,
         caseInsensitiveComparator>
         quarkdb::redis_cmd_map;

struct cmdMapInit {
  cmdMapInit() {
    redis_cmd_map["ping"] = {RedisCommand::PING, CommandType::CONTROL};
    redis_cmd_map["debug"] = {RedisCommand::DEBUG, CommandType::CONTROL};

    redis_cmd_map["get"] = {RedisCommand::GET, CommandType::READ};
    redis_cmd_map["exists"] = {RedisCommand::EXISTS, CommandType::READ};
    redis_cmd_map["keys"] =  {RedisCommand::KEYS, CommandType::READ};
    redis_cmd_map["scan"] =  {RedisCommand::SCAN, CommandType::READ};
    redis_cmd_map["hget"] = {RedisCommand::HGET, CommandType::READ};
    redis_cmd_map["hexists"] = {RedisCommand::HEXISTS, CommandType::READ};
    redis_cmd_map["hkeys"] = {RedisCommand::HKEYS, CommandType::READ};
    redis_cmd_map["hgetall"] = {RedisCommand::HGETALL, CommandType::READ};
    redis_cmd_map["hlen"] = {RedisCommand::HLEN, CommandType::READ};
    redis_cmd_map["hvals"] = {RedisCommand::HVALS, CommandType::READ};
    redis_cmd_map["hscan"] = {RedisCommand::HSCAN, CommandType::READ};
    redis_cmd_map["sismember"] = {RedisCommand::SISMEMBER, CommandType::READ};
    redis_cmd_map["smembers"] = {RedisCommand::SMEMBERS, CommandType::READ};
    redis_cmd_map["scard"] = {RedisCommand::SCARD, CommandType::READ};
    redis_cmd_map["sscan"] = {RedisCommand::SSCAN, CommandType::READ};
    redis_cmd_map["llen"] = {RedisCommand::LLEN, CommandType::READ};
    redis_cmd_map["config_get"] = {RedisCommand::CONFIG_GET, CommandType::READ};
    redis_cmd_map["config_getall"] = {RedisCommand::CONFIG_GETALL, CommandType::READ};

    redis_cmd_map["flushall"] = {RedisCommand::FLUSHALL, CommandType::WRITE};
    redis_cmd_map["set"] = {RedisCommand::SET, CommandType::WRITE};
    redis_cmd_map["del"] =  {RedisCommand::DEL, CommandType::WRITE};
    redis_cmd_map["hset"] =  {RedisCommand::HSET, CommandType::WRITE};
    redis_cmd_map["hmset"] =  {RedisCommand::HMSET, CommandType::WRITE};
    redis_cmd_map["hsetnx"] = {RedisCommand::HSETNX, CommandType::WRITE};
    redis_cmd_map["hincrby"] = {RedisCommand::HINCRBY, CommandType::WRITE};
    redis_cmd_map["hincrbyfloat"] = {RedisCommand::HINCRBYFLOAT, CommandType::WRITE};
    redis_cmd_map["hdel"] = {RedisCommand::HDEL, CommandType::WRITE};
    redis_cmd_map["sadd"] = {RedisCommand::SADD, CommandType::WRITE};
    redis_cmd_map["srem"] = {RedisCommand::SREM, CommandType::WRITE};
    redis_cmd_map["lpush"] = {RedisCommand::LPUSH, CommandType::WRITE};
    redis_cmd_map["lpop"] = {RedisCommand::LPOP, CommandType::WRITE};
    redis_cmd_map["rpush"] = {RedisCommand::RPUSH, CommandType::WRITE};
    redis_cmd_map["rpop"] = {RedisCommand::RPOP, CommandType::WRITE};
    redis_cmd_map["config_set"] = {RedisCommand::CONFIG_SET, CommandType::WRITE};

    redis_cmd_map["raft_handshake"] = {RedisCommand::RAFT_HANDSHAKE, CommandType::RAFT};
    redis_cmd_map["raft_append_entries"] = {RedisCommand::RAFT_APPEND_ENTRIES, CommandType::RAFT};
    redis_cmd_map["raft_info"] = {RedisCommand::RAFT_INFO, CommandType::RAFT};
    redis_cmd_map["raft_request_vote"] = {RedisCommand::RAFT_REQUEST_VOTE, CommandType::RAFT};
    redis_cmd_map["raft_fetch"] = {RedisCommand::RAFT_FETCH, CommandType::RAFT};
    redis_cmd_map["raft_checkpoint"] = {RedisCommand::RAFT_CHECKPOINT, CommandType::RAFT};
    redis_cmd_map["raft_attempt_coup"] = {RedisCommand::RAFT_ATTEMPT_COUP, CommandType::RAFT};
    redis_cmd_map["raft_add_observer"] = {RedisCommand::RAFT_ADD_OBSERVER, CommandType::RAFT};
    redis_cmd_map["raft_remove_member"] = {RedisCommand::RAFT_REMOVE_MEMBER, CommandType::RAFT};
    redis_cmd_map["raft_promote_observer"] = {RedisCommand::RAFT_PROMOTE_OBSERVER, CommandType::RAFT};

    redis_cmd_map["quarkdb_info"] = {RedisCommand::QUARKDB_INFO, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_stats"] = {RedisCommand::QUARKDB_STATS, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_detach"] = {RedisCommand::QUARKDB_DETACH, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_attach"] = {RedisCommand::QUARKDB_ATTACH, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_start_resilvering"] = {RedisCommand::QUARKDB_START_RESILVERING, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_finish_resilvering"] = {RedisCommand::QUARKDB_FINISH_RESILVERING, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_resilvering_copy_file"] = {RedisCommand::QUARKDB_RESILVERING_COPY_FILE, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_cancel_resilvering"] = {RedisCommand::QUARKDB_CANCEL_RESILVERING, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_bulkload_finalize"] = {RedisCommand::QUARKDB_BULKLOAD_FINALIZE, CommandType::QUARKDB};

  }
} cmd_map_init;
