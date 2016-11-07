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

#include "RaftTalker.hh"

using namespace quarkdb;

RaftTalker::RaftTalker(const RaftServer &server, const RaftClusterID &clusterID_)
: clusterID(clusterID_), tunnel(server.hostname, server.port, {"RAFT_HANDSHAKE", clusterID}) {

}

RaftTalker::RaftTalker(const RaftServer &server)
: tunnel(server.hostname, server.port) {
}

std::future<redisReplyPtr> RaftTalker::appendEntries(
  RaftTerm term, RaftServer leader, LogIndex prevIndex,
  RaftTerm prevTerm, LogIndex commit,
  const std::vector<RedisRequest> &reqs,
  const std::vector<RaftTerm> &entryTerms) {

  if(reqs.size() != entryTerms.size()) {
    qdb_throw("called appendEntries with mismatching sizes for reqs and entryTerms");
  }

  if(term < prevTerm) {
    qdb_throw(SSTR("term < prevTerm.. " << prevTerm << "," << term));
  }

  RedisRequest header;
  header.emplace_back("RAFT_APPEND_ENTRIES");
  header.emplace_back(std::to_string(term));
  header.emplace_back(leader.toString());
  header.emplace_back(std::to_string(prevIndex));
  header.emplace_back(std::to_string(prevTerm));
  header.emplace_back(std::to_string(commit));
  header.emplace_back(std::to_string(reqs.size()));

  size_t payloadSize = header.size();
  for(size_t i = 0; i < reqs.size(); i++) {
    payloadSize += 2 + reqs[i].size();
  }

  const char *payload[payloadSize];
  size_t sizes[payloadSize];

  // add header to payload
  for(size_t i = 0; i < header.size(); i++) {
    payload[i] = header[i].c_str();
    sizes[i] = header[i].size();
  }

  RedisRequest helper;
  helper.reserve(reqs.size()*2 + 10);

  size_t index = header.size();
  for(size_t i = 0; i < reqs.size(); i++) {
    if(i > 0 && entryTerms[i] < entryTerms[i-1]) {
      qdb_throw(SSTR("entryTerms went down .. i = " << i << ", entryTerms[i] = " << entryTerms[i] << ", entryTerms[i-1] == " << entryTerms[i-1]));
    }

    helper.emplace_back(std::to_string(reqs[i].size()));
    payload[index] = helper[helper.size()-1].c_str();
    sizes[index] = helper[helper.size()-1].size();

    if(term < entryTerms[i]) {
      qdb_throw(SSTR("term < entryTerms[i] .. i = " << i << ", term = " << term << ", entryTerms[i] = " << entryTerms[i]));
    }

    helper.emplace_back(std::to_string(entryTerms[i]));
    payload[index+1] = helper[helper.size()-1].c_str();
    sizes[index+1] = helper[helper.size()-1].size();

    for(size_t j = 0; j < reqs[i].size(); j++) {
      payload[index+j+2] = reqs[i][j].c_str();
      sizes[index+j+2] = reqs[i][j].size();
    }

    index += reqs[i].size() + 2;
  }

  return tunnel.execute(payloadSize, payload, sizes);
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
