// ----------------------------------------------------------------------
// File: Raft.cc
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

#include "../utils/IntToBinaryString.hh"
#include "RaftTalker.hh"
#include "RaftTimeouts.hh"
#include "../Version.hh"

using namespace quarkdb;

RaftHandshake::RaftHandshake(const RaftClusterID &clusterID_, const RaftTimeouts &timeouts_)
  : clusterID(clusterID_), timeouts(timeouts_) { }

std::vector<std::string> RaftHandshake::provideHandshake() {
  return {"RAFT_HANDSHAKE", VERSION_FULL_STRING, clusterID, timeouts.toString()};
}

bool RaftHandshake::validateResponse(const redisReplyPtr &reply) {
  if(!reply) {
    return false;
  }

  if(reply->type != REDIS_REPLY_STATUS) {
    return false;
  }

  if(std::string(reply->str, reply->len) != "OK") {
    return false;
  }

  return true;
}

RaftTalker::RaftTalker(const RaftServer &server_, const RaftClusterID &clusterID, const RaftTimeouts &timeouts)
: server(server_), tlsconfig(), tunnel(server.hostname, server.port, false, false, tlsconfig, std::unique_ptr<Handshake>(new RaftHandshake(clusterID, timeouts)) ) {

}

RaftTalker::RaftTalker(const RaftServer &server_)
: server(server_), tunnel(server.hostname, server.port) {
}

std::future<redisReplyPtr> RaftTalker::heartbeat(RaftTerm term, const RaftServer &leader) {
  RedisRequest payload;

  payload.emplace_back("RAFT_HEARTBEAT");
  payload.emplace_back(std::to_string(term));
  payload.emplace_back(leader.toString());

  return tunnel.execute(payload);
}

std::future<redisReplyPtr> RaftTalker::appendEntries(
  RaftTerm term, RaftServer leader, LogIndex prevIndex,
  RaftTerm prevTerm, LogIndex commit,
  const std::vector<RaftSerializedEntry> &entries) {

  if(term < prevTerm) {
    qdb_throw(SSTR("term < prevTerm.. " << prevTerm << "," << term));
  }

  RedisRequest payload;
  payload.reserve(3 + entries.size());

  payload.emplace_back("RAFT_APPEND_ENTRIES");
  payload.emplace_back(leader.toString());

  char buffer[sizeof(int64_t) * 5];
  intToBinaryString(term,             buffer + 0*sizeof(int64_t));
  intToBinaryString(prevIndex,        buffer + 1*sizeof(int64_t));
  intToBinaryString(prevTerm,         buffer + 2*sizeof(int64_t));
  intToBinaryString(commit,           buffer + 3*sizeof(int64_t));
  intToBinaryString(entries.size(),   buffer + 4*sizeof(int64_t));

  payload.emplace_back(buffer, 5*sizeof(int64_t));

  for(size_t i = 0; i < entries.size(); i++) {
    payload.push_back(entries[i]);
  }

  return tunnel.execute(payload);
}

std::future<redisReplyPtr> RaftTalker::requestVote(const RaftVoteRequest &req) {
  RedisRequest payload;

  payload.emplace_back("RAFT_REQUEST_VOTE");
  payload.emplace_back(std::to_string(req.term));
  payload.emplace_back(req.candidate.toString());
  payload.emplace_back(std::to_string(req.lastIndex));
  payload.emplace_back(std::to_string(req.lastTerm));

  return tunnel.execute(payload);
}

std::future<redisReplyPtr> RaftTalker::fetch(LogIndex index) {
  RedisRequest payload;

  payload.emplace_back("RAFT_FETCH");
  payload.emplace_back(std::to_string(index));

  return tunnel.execute(payload);
}

std::future<redisReplyPtr> RaftTalker::resilveringStart(const ResilveringEventID &id) {
  return tunnel.exec("quarkdb_start_resilvering", id);
}

std::future<redisReplyPtr> RaftTalker::resilveringCopy(const ResilveringEventID &id, const std::string &filename, const std::string &contents) {
  return tunnel.exec("quarkdb_resilvering_copy_file", id, filename, contents);
}

std::future<redisReplyPtr> RaftTalker::resilveringFinish(const ResilveringEventID &id) {
  return tunnel.exec("quarkdb_finish_resilvering", id);
}

std::future<redisReplyPtr> RaftTalker::resilveringCancel(const ResilveringEventID &id, const std::string &reason) {
  return tunnel.exec("quarkdb_cancel_resilvering");
}
