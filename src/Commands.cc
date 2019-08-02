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
         CommandComparator>
         quarkdb::redis_cmd_map;

struct cmdMapInit {
  cmdMapInit() {
    redis_cmd_map["ping"] = {RedisCommand::PING, CommandType::CONTROL};
    redis_cmd_map["debug"] = {RedisCommand::DEBUG, CommandType::CONTROL};
    redis_cmd_map["monitor"] = {RedisCommand::MONITOR, CommandType::CONTROL};
    redis_cmd_map["client_id"] = {RedisCommand::CLIENT_ID, CommandType::CONTROL};
    redis_cmd_map["command_stats"] = {RedisCommand::COMMAND_STATS, CommandType::CONTROL};

    redis_cmd_map["auth"] = {RedisCommand::AUTH, CommandType::AUTHENTICATION};
    redis_cmd_map["hmac_auth_generate_challenge"] = {RedisCommand::HMAC_AUTH_GENERATE_CHALLENGE, CommandType::AUTHENTICATION};
    redis_cmd_map["hmac_auth_validate_challenge"] = {RedisCommand::HMAC_AUTH_VALIDATE_CHALLENGE, CommandType::AUTHENTICATION};

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
    redis_cmd_map["deque_len"] = {RedisCommand::DEQUE_LEN, CommandType::READ};
    redis_cmd_map["deque_scan_back"] = {RedisCommand::DEQUE_SCAN_BACK, CommandType::READ};
    redis_cmd_map["config_get"] = {RedisCommand::CONFIG_GET, CommandType::READ};
    redis_cmd_map["config_getall"] = {RedisCommand::CONFIG_GETALL, CommandType::READ};
    redis_cmd_map["lhget"] = {RedisCommand::LHGET, CommandType::READ};
    redis_cmd_map["lhlen"] = {RedisCommand::LHLEN, CommandType::READ};
    redis_cmd_map["lhscan"] = {RedisCommand::LHSCAN, CommandType::READ};
    redis_cmd_map["lhget_with_fallback"] = {RedisCommand::LHGET_WITH_FALLBACK, CommandType::READ};
    redis_cmd_map["raw_scan_tombstones"] = {RedisCommand::RAW_SCAN_TOMBSTONES, CommandType::READ};
    redis_cmd_map["raw_scan"] = {RedisCommand::RAW_SCAN, CommandType::READ};
    redis_cmd_map["raw_get_all_versions"] = {RedisCommand::RAW_GET_ALL_VERSIONS, CommandType::READ};
    redis_cmd_map["clock_get"] = {RedisCommand::CLOCK_GET, CommandType::READ};
    redis_cmd_map["type"] = {RedisCommand::TYPE, CommandType::READ};
    redis_cmd_map["vhgetall"] = {RedisCommand::VHGETALL, CommandType::READ};
    redis_cmd_map["vhlen"] = {RedisCommand::VHLEN, CommandType::READ};
    redis_cmd_map["lease_get_pending_expiration_events"] = {RedisCommand::LEASE_GET_PENDING_EXPIRATION_EVENTS, CommandType::READ};

    redis_cmd_map["flushall"] = {RedisCommand::FLUSHALL, CommandType::WRITE};
    redis_cmd_map["set"] = {RedisCommand::SET, CommandType::WRITE};
    redis_cmd_map["del"] =  {RedisCommand::DEL, CommandType::WRITE};
    redis_cmd_map["hset"] =  {RedisCommand::HSET, CommandType::WRITE};
    redis_cmd_map["hmset"] =  {RedisCommand::HMSET, CommandType::WRITE};
    redis_cmd_map["hsetnx"] = {RedisCommand::HSETNX, CommandType::WRITE};
    redis_cmd_map["hincrby"] = {RedisCommand::HINCRBY, CommandType::WRITE};
    redis_cmd_map["hincrbyfloat"] = {RedisCommand::HINCRBYFLOAT, CommandType::WRITE};
    redis_cmd_map["hincrbymulti"] = {RedisCommand::HINCRBYMULTI, CommandType::WRITE};
    redis_cmd_map["hdel"] = {RedisCommand::HDEL, CommandType::WRITE};
    redis_cmd_map["hclone"] = {RedisCommand::HCLONE, CommandType::WRITE};
    redis_cmd_map["sadd"] = {RedisCommand::SADD, CommandType::WRITE};
    redis_cmd_map["srem"] = {RedisCommand::SREM, CommandType::WRITE};
    redis_cmd_map["smove"] = {RedisCommand::SMOVE, CommandType::WRITE};
    redis_cmd_map["deque_push_front"] = {RedisCommand::DEQUE_PUSH_FRONT, CommandType::WRITE};
    redis_cmd_map["deque_pop_front"] = {RedisCommand::DEQUE_POP_FRONT, CommandType::WRITE};
    redis_cmd_map["deque_push_back"] = {RedisCommand::DEQUE_PUSH_BACK, CommandType::WRITE};
    redis_cmd_map["deque_pop_back"] = {RedisCommand::DEQUE_POP_BACK, CommandType::WRITE};
    redis_cmd_map["deque_trim_front"] = {RedisCommand::DEQUE_TRIM_FRONT, CommandType::WRITE};
    redis_cmd_map["deque_clear"] = {RedisCommand::DEQUE_CLEAR, CommandType::WRITE};
    redis_cmd_map["config_set"] = {RedisCommand::CONFIG_SET, CommandType::WRITE};
    redis_cmd_map["lhset"] = {RedisCommand::LHSET, CommandType::WRITE};
    redis_cmd_map["lhdel"] = {RedisCommand::LHDEL, CommandType::WRITE};
    redis_cmd_map["lhmset"] = {RedisCommand::LHMSET, CommandType::WRITE};
    redis_cmd_map["lhdel_with_fallback"] = {RedisCommand::LHDEL_WITH_FALLBACK, CommandType::WRITE};
    redis_cmd_map["lhset_and_del_fallback"] = {RedisCommand::LHSET_AND_DEL_FALLBACK, CommandType::WRITE};
    redis_cmd_map["convert_hash_field_to_lhash"] = {RedisCommand::CONVERT_HASH_FIELD_TO_LHASH, CommandType::WRITE};
    redis_cmd_map["lease_acquire"] = {RedisCommand::LEASE_ACQUIRE, CommandType::WRITE};
    redis_cmd_map["lease_get"] = {RedisCommand::LEASE_GET, CommandType::WRITE};
    redis_cmd_map["lease_release"] = {RedisCommand::LEASE_RELEASE, CommandType::WRITE};
    redis_cmd_map["timestamped_lease_acquire"] = {RedisCommand::TIMESTAMPED_LEASE_ACQUIRE, CommandType::WRITE};
    redis_cmd_map["timestamped_lease_get"] = {RedisCommand::TIMESTAMPED_LEASE_GET, CommandType::WRITE};
    redis_cmd_map["timestamped_lease_release"] = {RedisCommand::TIMESTAMPED_LEASE_RELEASE, CommandType::WRITE};
    redis_cmd_map["vhset"] = {RedisCommand::VHSET, CommandType::WRITE};
    redis_cmd_map["vhdel"] = {RedisCommand::VHDEL, CommandType::WRITE};

    redis_cmd_map["exec"] = {RedisCommand::EXEC, CommandType::CONTROL};
    redis_cmd_map["discard"] = {RedisCommand::DISCARD, CommandType::CONTROL};
    redis_cmd_map["multi"] = {RedisCommand::MULTI, CommandType::CONTROL};
    redis_cmd_map["tx_readonly"] = {RedisCommand::TX_READONLY, CommandType::READ};
    redis_cmd_map["tx_readwrite"] = {RedisCommand::TX_READWRITE, CommandType::WRITE};

    // These have been retained for compatibility, to ensure old raft journal
    // entries can still be processed correctly. TODO: Remove after a couple of releases.
    redis_cmd_map["multiop_read"] = {RedisCommand::TX_READONLY, CommandType::READ};
    redis_cmd_map["multiop_readwrite"] = {RedisCommand::TX_READWRITE, CommandType::WRITE};

    redis_cmd_map["raft_handshake"] = {RedisCommand::RAFT_HANDSHAKE, CommandType::RAFT};
    redis_cmd_map["raft_append_entries"] = {RedisCommand::RAFT_APPEND_ENTRIES, CommandType::RAFT};
    redis_cmd_map["raft_info"] = {RedisCommand::RAFT_INFO, CommandType::RAFT};
    redis_cmd_map["raft_leader_info"] = {RedisCommand::RAFT_LEADER_INFO, CommandType::RAFT};
    redis_cmd_map["raft_request_vote"] = {RedisCommand::RAFT_REQUEST_VOTE, CommandType::RAFT};
    redis_cmd_map["raft_fetch"] = {RedisCommand::RAFT_FETCH, CommandType::RAFT};
    redis_cmd_map["raft_attempt_coup"] = {RedisCommand::RAFT_ATTEMPT_COUP, CommandType::RAFT};
    redis_cmd_map["raft_add_observer"] = {RedisCommand::RAFT_ADD_OBSERVER, CommandType::RAFT};
    redis_cmd_map["raft_remove_member"] = {RedisCommand::RAFT_REMOVE_MEMBER, CommandType::RAFT};
    redis_cmd_map["raft_promote_observer"] = {RedisCommand::RAFT_PROMOTE_OBSERVER, CommandType::RAFT};
    redis_cmd_map["raft_heartbeat"] = {RedisCommand::RAFT_HEARTBEAT, CommandType::RAFT};
    redis_cmd_map["raft_fetch_last"] = {RedisCommand::RAFT_FETCH_LAST, CommandType::RAFT};
    redis_cmd_map["raft_journal_scan"] = {RedisCommand::RAFT_JOURNAL_SCAN, CommandType::RAFT};

    redis_cmd_map["activate_stale_reads"] = {RedisCommand::ACTIVATE_STALE_READS, CommandType::RAFT};

    redis_cmd_map["quarkdb_info"] = {RedisCommand::QUARKDB_INFO, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_detach"] = {RedisCommand::QUARKDB_DETACH, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_attach"] = {RedisCommand::QUARKDB_ATTACH, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_start_resilvering"] = {RedisCommand::QUARKDB_START_RESILVERING, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_finish_resilvering"] = {RedisCommand::QUARKDB_FINISH_RESILVERING, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_resilvering_copy_file"] = {RedisCommand::QUARKDB_RESILVERING_COPY_FILE, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_cancel_resilvering"] = {RedisCommand::QUARKDB_CANCEL_RESILVERING, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_bulkload_finalize"] = {RedisCommand::QUARKDB_BULKLOAD_FINALIZE, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_invalid_command"] = {RedisCommand::QUARKDB_INVALID_COMMAND, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_manual_compaction"] = {RedisCommand::QUARKDB_MANUAL_COMPACTION, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_level_stats"] = {RedisCommand::QUARKDB_LEVEL_STATS, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_compression_stats"] = {RedisCommand::QUARKDB_COMPRESSION_STATS, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_version"] = {RedisCommand::QUARKDB_VERSION, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_checkpoint"] = {RedisCommand::QUARKDB_CHECKPOINT, CommandType::QUARKDB};
    redis_cmd_map["quarkdb_health_local"] = {RedisCommand::QUARKDB_HEALTH_LOCAL, CommandType::QUARKDB};

    // Compatibility: Keep raft_checkpoint, make identical to quarkdb_checkpoint.
    // Maybe remove in a few versions.
    redis_cmd_map["raft_checkpoint"] = {RedisCommand::QUARKDB_CHECKPOINT, CommandType::QUARKDB};

    redis_cmd_map["recovery_info"] = {RedisCommand::RECOVERY_INFO, CommandType::RECOVERY};
    redis_cmd_map["recovery_set"] = {RedisCommand::RECOVERY_SET, CommandType::RECOVERY};
    redis_cmd_map["recovery_get"] = {RedisCommand::RECOVERY_GET, CommandType::RECOVERY};
    redis_cmd_map["recovery_del"] = {RedisCommand::RECOVERY_DEL, CommandType::RECOVERY};
    redis_cmd_map["recovery_force_reconfigure_journal"] = {RedisCommand::RECOVERY_FORCE_RECONFIGURE_JOURNAL, CommandType::RECOVERY};
    redis_cmd_map["recovery_scan"] = {RedisCommand::RECOVERY_SCAN, CommandType::RECOVERY};
    redis_cmd_map["recovery_get_all_versions"] = {RedisCommand::RECOVERY_GET_ALL_VERSIONS, CommandType::RECOVERY};

    redis_cmd_map["convert_string_to_int"] = {RedisCommand::CONVERT_STRING_TO_INT, CommandType::CONTROL};
    redis_cmd_map["convert_int_to_string"] = {RedisCommand::CONVERT_INT_TO_STRING, CommandType::CONTROL};

    redis_cmd_map["publish"] = {RedisCommand::PUBLISH, CommandType::PUBSUB};
    redis_cmd_map["subscribe"] = {RedisCommand::SUBSCRIBE, CommandType::PUBSUB};
    redis_cmd_map["psubscribe"] = {RedisCommand::PSUBSCRIBE, CommandType::PUBSUB};
    redis_cmd_map["unsubscribe"] = {RedisCommand::UNSUBSCRIBE, CommandType::PUBSUB};
    redis_cmd_map["punsubscribe"] = {RedisCommand::PUNSUBSCRIBE, CommandType::PUBSUB};
  }
} cmd_map_init;
