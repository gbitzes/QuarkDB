// ----------------------------------------------------------------------
// File: RaftDispatcher.cc
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

#include "RaftDispatcher.hh"
#include "../Response.hh"
#include "RaftUtils.hh"

#include <random>
#include <sys/stat.h>

using namespace quarkdb;

RaftDispatcher::RaftDispatcher(RaftJournal &jour, RocksDB &sm, RaftState &st, RaftClock &rc)
: journal(jour), stateMachine(sm), state(st), raftClock(rc), redisDispatcher(sm) {
}

LinkStatus RaftDispatcher::dispatch(Connection *conn, RedisRequest &req, LogIndex commit) {
  if(commit != 0) qdb_throw("commit should have been zero here: " << commit);

  auto it = redis_cmd_map.find(req[0]);
  if(it == redis_cmd_map.end()) return conn->err(SSTR("unknown command " << quotes(req[0])));

  RedisCommand cmd = it->second.first;
  switch(cmd) {
    case RedisCommand::RAFT_INFO: {
      // safe, read-only request, does not need authorization
      return conn->vector(this->info().toVector());
    }
    case RedisCommand::RAFT_FETCH: {
      // safe, read-only request, does not need authorization
      if(req.size() != 2) return conn->errArgs(req[0]);

      LogIndex index;
      if(!my_strtoll(req[1], index)) return conn->err(SSTR("could not parse " << req[1]));

      RaftEntry entry;
      std::vector<std::string> ret;

      if(this->fetch(index, entry)) {
        ret.emplace_back(std::to_string(entry.term));
        for(size_t i = 0; i < entry.request.size(); i++) {
          ret.emplace_back(entry.request[i]);
        }
      }

      return conn->vector(ret);
    }
    case RedisCommand::RAFT_APPEND_ENTRIES: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      RaftAppendEntriesRequest dest;
      if(!RaftParser::appendEntries(std::move(req), dest)) {
        return conn->err("malformed request");
      }

      RaftAppendEntriesResponse resp = appendEntries(std::move(dest));
      return conn->vector(resp.toVector());
    }
    case RedisCommand::RAFT_REQUEST_VOTE: {
      if(!conn->raftAuthorization) return conn->err("not authorized to issue raft commands");
      RaftVoteRequest votereq;
      if(!RaftParser::voteRequest(req, votereq)) {
        return conn->err("malformed request");
      }

      RaftVoteResponse resp = requestVote(votereq);
      return conn->vector(resp.toVector());
    }
    case RedisCommand::RAFT_HANDSHAKE: {
      conn->raftAuthorization = false;
      if(req.size() != 2) return conn->errArgs(req[0]);
      if(req[1] != journal.getClusterID()) {
        qdb_critical("received handshake with wrong cluster id: " << req[1] << " (mine is " << journal.getClusterID() << ")");
        return conn->err("wrong cluster id");
      }

      conn->raftAuthorization = true;
      return conn->ok();
    }
    case RedisCommand::RAFT_CHECKPOINT: {
      if(req.size() != 2) return conn->errArgs(req[0]);

      std::string err;
      if(!checkpoint(req[1], err)) {
        return conn->err(err);
      }

      return conn->ok();
    }
    case RedisCommand::RAFT_COUP_DETAT: {
      RaftStateSnapshot snapshot = state.getSnapshot();

      if(snapshot.leader.empty()) {
        return conn->err("I have no leader to depose of");
      }

      if(snapshot.leader == state.getMyself()) {
        return conn->err("I am the leader! I can't revolt against myself, you know.");
      }

      qdb_event("Received request to attempt a coup d'etat against the current leader.");
      raftClock.triggerTimeout();
      return conn->status("vive la revolution");
    }
    case RedisCommand::RAFT_ADD_OBSERVER:
    case RedisCommand::RAFT_REMOVE_MEMBER:
    case RedisCommand::RAFT_PROMOTE_OBSERVER: {
      if(req.size() != 2) return conn->errArgs(req[0]);

      RaftServer srv;
      if(!parseServer(req[1], srv)) {
        return conn->err(SSTR("cannot parse server: " << req[1]));
      }

      RaftStateSnapshot snapshot = state.getSnapshot();
      if(snapshot.status != RaftStatus::LEADER) return conn->err("not a leader");
      if(srv == state.getMyself()) conn->err(SSTR("cannot perform membership changes on current leader"));

      std::string err;
      bool rc;

      if(cmd == RedisCommand::RAFT_ADD_OBSERVER) {
        rc = journal.addObserver(snapshot.term, srv, err);
      }
      else if(cmd == RedisCommand::RAFT_REMOVE_MEMBER) {
        rc = journal.removeMember(snapshot.term, srv, err);
      }
      else if(cmd == RedisCommand::RAFT_PROMOTE_OBSERVER) {
        rc = journal.promoteObserver(snapshot.term, srv, err);
      }
      else {
        qdb_throw("should never happen");
      }

      if(!rc) return conn->err(err);
      return conn->ok();
    }
    default: {
      return this->service(conn, req, cmd, it->second.second);
    }
  }
}

LinkStatus RaftDispatcher::service(Connection *conn, RedisRequest &req, RedisCommand &cmd, CommandType &type) {
  // control command, service even if unavailable
  if(type == CommandType::CONTROL) {
    return conn->appendReq(&redisDispatcher, std::move(req));
    // return serviceRead(conn, req, cmd, type);
  }

  // if not leader, redirect
  RaftStateSnapshot snapshot = state.getSnapshot();
  if(snapshot.status != RaftStatus::LEADER) {
    if(snapshot.leader.empty()) {
      return conn->appendError("unavailable");
    }
    return conn->appendError(SSTR("MOVED 0 " << snapshot.leader.toString()));
  }

  // read request, easy case
  if(type == CommandType::READ || type == CommandType::CONTROL) {
    return conn->appendReq(&redisDispatcher, std::move(req));
    // return serviceRead(conn, req, cmd, type);
  }

  // write request, must append to raft log
  std::lock_guard<std::mutex> lock(raftCommand);

  LogIndex index = journal.getLogSize();
  if(!journal.append(index, snapshot.term, req)) {
    qdb_critical("appending to journal failed for index = " << index <<
    " and term " << snapshot.term << " when servicing client request");
    return conn->appendError("unknown error");
  }

  conn->appendReq(&redisDispatcher, std::move(req), index);
  blockedWrites[index] = conn;
  return 1;
}

void RaftDispatcher::flushQueues(const std::string &msg) {
  for(auto it = blockedWrites.begin(); it != blockedWrites.end(); it++) {
    it->second->flushPending(msg);
  }
  blockedWrites.clear();
}

LinkStatus RaftDispatcher::applyCommits(LogIndex commitIndex) {
  std::lock_guard<std::mutex> lock(raftCommand);

  for(LogIndex index = stateMachine.getLastApplied()+1; index <= commitIndex; index++) {
    applyOneCommit(index);
  }

  return 1;
}

LinkStatus RaftDispatcher::applyOneCommit(LogIndex index) {

  auto it = blockedWrites.find(index);
  if(it == blockedWrites.end()) {
    // this journal entry is not related to any connection,
    // let's just apply it manually from the journal
    // This happens in followers.

    RaftEntry entry;

    if(!journal.fetch(index, entry).ok()) {
      // serious error, threatens consistency. Bail out
      qdb_throw("failed to fetch log entry " << index << " when applying commits");
    }

    Connection devnull;
    redisDispatcher.dispatch(&devnull, entry.request, index);
    return 1;
  }

  Connection *conn = it->second;

  LogIndex newBlockingIndex = conn->dispatchPending(&redisDispatcher, index);
  if(newBlockingIndex > 0) {
    blockedWrites[newBlockingIndex] = conn;
  }
  blockedWrites.erase(it);
  return 1;
}

RaftAppendEntriesResponse RaftDispatcher::appendEntries(RaftAppendEntriesRequest &&req) {
  std::lock_guard<std::mutex> lock(raftCommand);
  if(req.leader == state.getMyself()) {
    qdb_throw("received appendEntries from myself");
  }

  state.observed(req.term, req.leader);
  RaftStateSnapshot snapshot = state.getSnapshot();

  if(state.inShutdown()) return {snapshot.term, journal.getLogSize(), false, "in shutdown"};
  if(req.term < snapshot.term) {
    return {snapshot.term, journal.getLogSize(), false, "My raft term is newer"};
  }

  if(req.term == snapshot.term && req.leader != snapshot.leader) {
    qdb_throw("Received append entries from " << req.leader.toString() << ", while I believe leader for term " << snapshot.term << " is " << snapshot.leader.toString());
    // TODO trigger panic?
    return {snapshot.term, journal.getLogSize(), false, "You are not the current leader"};
  }

  flushQueues(SSTR("MOVED 0 " << snapshot.leader.toString()));
  raftClock.heartbeat();

  if(!journal.matchEntries(req.prevIndex, req.prevTerm)) {
    return {snapshot.term, journal.getLogSize(), false, "Log entry mismatch"};
  }

  //----------------------------------------------------------------------------
  // Four cases.
  // 1. All entries are new; we're grand. By far the most common case.
  // 2. The leader is sligthly confused and is sending entries that I have
  //    already. Perform a quick check to ensure they're identical to mine and
  //    continue on like nothing happened.
  // 3. Some of the entries are different than mine. This can be caused by mild
  //    log inconsistencies when switching leaders. This is normal and expected
  //    to happen rarely, so let's remove the inconsistent entries.
  // 4. Some of the entries are different, AND they've already been committed
  //    or applied. This is a major safety violation and should never happen.
  //----------------------------------------------------------------------------

  LogIndex firstInconsistency = journal.compareEntries(req.prevIndex+1, req.entries);
  LogIndex appendFrom = firstInconsistency - (req.prevIndex+1);

  // check if ALL entries are duplicates. If so, I don't need to do anything.
  if(appendFrom < LogIndex(req.entries.size()) ) {
    if(firstInconsistency <= journal.getCommitIndex()) {
      qdb_throw("detected inconsistent entries for index " << firstInconsistency << ". "
      << " Leader attempted to overwrite a committed entry with one with different contents.");
    }

    journal.removeEntries(firstInconsistency);

    for(size_t i = appendFrom; i < req.entries.size(); i++) {
      if(!journal.append(req.prevIndex+1+i, req.entries[i].term, req.entries[i].request)) {
        qdb_warn("something odd happened when adding entries to the journal.. probably a race condition, but should be harmless");
        return {snapshot.term, journal.getLogSize(), false, "Unknown error"};
      }
    }
  }

  journal.setCommitIndex(std::min(journal.getLogSize()-1, req.commitIndex));
  return {snapshot.term, journal.getLogSize(), true, ""};
}

RaftVoteResponse RaftDispatcher::requestVote(RaftVoteRequest &req) {
  std::lock_guard<std::mutex> lock(raftCommand);
  if(req.candidate == state.getMyself()) {
    qdb_throw("received request vote from myself: " << state.getMyself().toString());
  }

  //----------------------------------------------------------------------------
  // Defend against disruptive servers.
  // A node that has been removed from the cluster will often not know that,
  // and will repeatedly start elections trying to depose the current leader,
  // effectively making the cluster unavailable.
  //
  // If this node is not part of the cluster (that I know of) and I am already
  // in contact with the leader, completely ignore its vote request, and don't
  // take its term into consideration.
  //
  // If I don't have a leader, the situation is different though. Maybe this
  // node was added later, and I just don't know about it yet. Process the
  // request normally, since there's no leader to depose of, anyway.
  //----------------------------------------------------------------------------

  if(!contains(state.getNodes(), req.candidate)) {
    RaftStateSnapshot snapshot = state.getSnapshot();
    if(!snapshot.leader.empty()) {
      qdb_critical("Non-voting " << req.candidate.toString() << " attempted to disrupt the cluster by starting an election for term " << req.term << ". Ignoring its request.");
      return {snapshot.term, false};
    }
    qdb_warn("Non-voting " << req.candidate.toString() << " is requesting a vote, even though it is not a voting member of the cluster as far I know. Will still process its request, since I have no leader.");
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
  if(!journal.fetch(myLastIndex, myLastTerm).ok()) {
    qdb_critical("Error when reading journal entry " << myLastIndex << " when processing request vote.");
    return {snapshot.term, false};
  }

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
  else {
    raftClock.heartbeat();
  }

  return {snapshot.term, granted};
}

RaftInfo RaftDispatcher::info() {
  std::lock_guard<std::mutex> lock(raftCommand);
  RaftStateSnapshot snapshot = state.getSnapshot();
  RaftMembership membership = journal.getMembership();

  return {journal.getClusterID(), state.getMyself(), snapshot.leader, membership.epoch, membership.nodes, membership.observers, snapshot.term, journal.getLogStart(),
          journal.getLogSize(), snapshot.status, journal.getCommitIndex(), stateMachine.getLastApplied(), blockedWrites.size()};
}

bool RaftDispatcher::fetch(LogIndex index, RaftEntry &entry) {
  entry = {};
  rocksdb::Status st = journal.fetch(index, entry);
  return st.ok();
}

bool RaftDispatcher::checkpoint(const std::string &path, std::string &err) {
  if(mkdir(path.c_str(), 0775) != 0) {
    err = SSTR("Error when creating directory '" << path << "', errno: " << errno);
    return false;
  }

  rocksdb::Status st = stateMachine.checkpoint(SSTR(path << "/state-machine"));
  if(!st.ok()) {
    err = st.ToString();
    return false;
  }

  st = journal.checkpoint(SSTR(path << "/raft-journal"));
  if(!st.ok()) {
    err = st.ToString();
    return false;
  }

  return true;
}
