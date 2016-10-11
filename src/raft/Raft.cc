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

#include "Raft.hh"
#include "../Response.hh"
#include "RaftUtils.hh"

#include <random>
using namespace quarkdb;

Raft::Raft(RaftJournal &jour, RocksDB &sm, RaftState &st, RaftClock &rc)
: journal(jour), stateMachine(sm), state(st), raftClock(rc) {
}

RaftState* Raft::getState() {
  return &state;
}

LinkStatus Raft::dispatch(Link *link, RedisRequest &req) {
  auto it = redis_cmd_map.find(req[0]);
  if(it == redis_cmd_map.end()) return Response::err(link, SSTR("unknown command " << quotes(req[0])));

  RedisCommand cmd = it->second.first;
  switch(cmd) {
    default: {
      return this->service(link, req, cmd, it->second.second);
    }
    case RedisCommand::RAFT_INFO: {
      return Response::vector(link, this->info().toVector());
    }
    case RedisCommand::RAFT_APPEND_ENTRIES: {
      RaftAppendEntriesRequest dest;
      if(!RaftParser::appendEntries(std::move(req), dest)) {
        return Response::err(link, "malformed request");
      }

      RaftAppendEntriesResponse resp = appendEntries(std::move(dest));
      return Response::vector(link, resp.toVector());
    }
    case RedisCommand::RAFT_REQUEST_VOTE: {
      RaftVoteRequest votereq;
      if(!RaftParser::voteRequest(req, votereq)) {
        return Response::err(link, "malformed request");
      }

      RaftVoteResponse resp = requestVote(votereq);
      return Response::vector(link, resp.toVector());
    }
    case RedisCommand::RAFT_HANDSHAKE: {
      return Response::ok(link);
    }
  }
}

LinkStatus Raft::service(Link *link, RedisRequest &req, RedisCommand &cmd, CommandType &type) {
  return 0;
}

RaftAppendEntriesResponse Raft::appendEntries(RaftAppendEntriesRequest &&req) {
  std::lock_guard<std::mutex> lock(raftCommand);
  if(req.leader == state.getMyself()) {
    qdb_throw("received appendEntries from myself");
  }

  state.observed(req.term, req.leader);
  RaftStateSnapshot snapshot = state.getSnapshot();

  if(req.term < snapshot.term) {
    return {snapshot.term, journal.getLogSize(), false, "My raft term is newer"};
  }

  if(req.term == snapshot.term && req.leader != snapshot.leader) {
    qdb_critical("Received append entries from " << req.leader.toString() << ", while I believe leader for term " << snapshot.term << " is " << req.leader.toString());
    // TODO trigger panic?
    return {snapshot.term, journal.getLogSize(), false, "You are not the current leader"};
  }

  if(!journal.matchEntries(req.prevIndex, req.prevTerm)) {
    return {snapshot.term, journal.getLogSize(), false, "Log entry mismatch"};
  }

  raftClock.heartbeat();

  // entry already exists?
  if(req.prevIndex+1 < journal.getLogSize()) {
    journal.removeEntries(req.prevIndex+1);
  }

  for(size_t i = 0; i < req.entries.size(); i++) {
    if(!journal.append(req.prevIndex+1+i, req.entries[i].term, req.entries[i].request)) {
      qdb_warn("something odd happened when adding entries to the journal.. probably a race condition, but should be harmless");
      return {snapshot.term, journal.getLogSize(), false, "Unknown error"};
    }
  }

  return {snapshot.term, journal.getLogSize(), true, ""};
}

RaftVoteResponse Raft::requestVote(RaftVoteRequest &req) {
  std::lock_guard<std::mutex> lock(raftCommand);
  if(req.candidate == state.getMyself()) {
    qdb_throw("received request vote from myself");
  }

  state.observed(req.term, {});
  RaftStateSnapshot snapshot = state.getSnapshot();

  if(snapshot.term != req.term) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " because of a term mismatch: " << snapshot.term << " vs " << req.term);
    return {snapshot.term, false};
  }

  if(!snapshot.votedFor.empty() && snapshot.votedFor != req.candidate) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " since I've voted already in this term (" << snapshot.term << ") for " << snapshot.votedFor.toString());
    return {snapshot.term, false};
  }

  LogIndex myLastIndex = journal.getLogSize()-1;
  RaftTerm myLastTerm;
  journal.fetch(myLastIndex, myLastTerm);

  if(req.lastTerm < myLastTerm) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " since my log is more up-to-date, based on last term: " << myLastIndex << "," << myLastTerm << " vs " << req.lastIndex << "," << req.lastTerm);
    return {snapshot.term, false};
  }

  if(req.lastIndex < myLastIndex) {
    qdb_event("Rejecting vote request from " << req.candidate.toString() << " since my log is more up-to-date, based on last index: " << myLastIndex << "," << myLastTerm << " vs " << req.lastIndex << "," << req.lastTerm);
    return {snapshot.term, false};
  }

  // grant vote
  bool granted = state.grantVote(req.term, req.candidate);
  if(!granted) {
    qdb_event("RaftState rejected the vote request from " << req.candidate.toString() << " and term " << req.term);
  }

  return {snapshot.term, granted};
}

RaftInfo Raft::info() {
  std::lock_guard<std::mutex> lock(raftCommand);
  RaftStateSnapshot snapshot = state.getSnapshot();
  return {journal.getClusterID(), state.getMyself(), snapshot.term, journal.getLogSize(), snapshot.status};
}

bool Raft::fetch(LogIndex index, RaftEntry &entry) {
  entry = {};
  rocksdb::Status st = journal.fetch(index, entry.term, entry.request);
  return st.ok();
}
