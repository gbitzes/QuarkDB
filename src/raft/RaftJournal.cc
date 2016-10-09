// ----------------------------------------------------------------------
// File: RaftJournal.cc
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

#include "RaftJournal.hh"
#include "../Common.hh"
#include "../Utils.hh"
#include "RaftState.hh"

using namespace quarkdb;

//------------------------------------------------------------------------------
// Helper functions
//------------------------------------------------------------------------------

static void append_int_to_string(int64_t source, std::ostringstream &target) {
  char buff[sizeof(source)];
  memcpy(&buff, &source, sizeof(source));
  target.write(buff, sizeof(source));
}

static int64_t fetch_int_from_string(const char *pos) {
  int64_t result;
  memcpy(&result, pos, sizeof(result));
  return result;
}

static std::string serializeRedisRequest(RaftTerm term, const RedisRequest &cmd) {
  std::ostringstream ss;
  append_int_to_string(term, ss);

  for(size_t i = 0; i < cmd.size(); i++) {
    append_int_to_string(cmd[i].size(), ss);
    ss << cmd[i];
  }

  return ss.str();
}

static void deserializeRedisRequest(const std::string &data, RaftTerm &term, RedisRequest &cmd) {
  cmd.clear();
  term = fetch_int_from_string(data.c_str());

  const char *pos = data.c_str() + sizeof(term);
  const char *end = data.c_str() + data.size();

  while(pos < end) {
    int64_t len = fetch_int_from_string(pos);
    pos += sizeof(len);

    cmd.emplace_back(pos, len);
    pos += len;
  }
}

//------------------------------------------------------------------------------
// RaftJournal
//------------------------------------------------------------------------------

void RaftJournal::ObliterateAndReinitializeJournal(const std::string &path, RaftClusterID clusterID, std::vector<RaftServer> nodes) {
  RocksDB store(path);
  return ObliterateAndReinitializeJournal(store, clusterID, nodes);
}

void RaftJournal::ObliterateAndReinitializeJournal(RocksDB &store, RaftClusterID clusterID, std::vector<RaftServer> nodes) {
  store.flushall();

  store.set_or_die("RAFT_CURRENT_TERM", "0");
  store.set_or_die("RAFT_LOG_SIZE", "1");
  store.set_or_die("RAFT_CLUSTER_ID", clusterID);
  store.set_or_die("RAFT_VOTED_FOR", "");
  store.set_or_die("RAFT_LAST_APPLIED", "0");

  RedisRequest req { "UPDATE_RAFT_NODES", serializeNodes(nodes) };
  store.set_or_die("RAFT_ENTRY_0", serializeRedisRequest(0, req));
  store.set_or_die("RAFT_NODES", serializeNodes(nodes));
  store.set_or_die("RAFT_OBSERVERS", "");
}


void RaftJournal::obliterate(RaftClusterID newClusterID, const std::vector<RaftServer> &newNodes) {
  ObliterateAndReinitializeJournal(store, newClusterID, newNodes);
  initialize();
}

void RaftJournal::initialize() {
  currentTerm = store.get_int_or_die("RAFT_CURRENT_TERM");
  logSize = store.get_int_or_die("RAFT_LOG_SIZE");
  clusterID = store.get_or_die("RAFT_CLUSTER_ID");
  lastApplied = store.get_int_or_die("RAFT_LAST_APPLIED");
  std::string vote = store.get_or_die("RAFT_VOTED_FOR");
  this->fetch_or_die(logSize-1, termOfLastEntry);

  std::string tmp = store.get_or_die("RAFT_NODES");
  if(!parseServers(tmp, nodes)) {
    qdb_throw("journal corruption, cannot parse RAFT_NODES: " << tmp);
  }

  tmp = store.get_or_die("RAFT_OBSERVERS");
  if(!tmp.empty() && !parseServers(tmp, observers)) {
    qdb_throw("journal corruption, cannot parse RAFT_OBSERVERS: " << tmp);
  }

  if(!vote.empty() && !parseServer(vote, votedFor)) {
    qdb_throw("journal corruption, cannot parse RAFT_VOTED_FOR: " << vote);
  }
}

RaftJournal::RaftJournal(const std::string &filename, RaftClusterID clusterID, const std::vector<RaftServer> &nodes)
: store(filename) {
  ObliterateAndReinitializeJournal(store, clusterID, nodes);
  initialize();
}

RaftJournal::RaftJournal(const std::string &filename) : store(filename) {
  initialize();
}

bool RaftJournal::setCurrentTerm(RaftTerm term, RaftServer vote) {
  std::lock_guard<std::mutex> lock(currentTermMutex);

  //------------------------------------------------------------------------------
  // Terms should never go back in time
  //------------------------------------------------------------------------------

  if(term < currentTerm) {
    return false;
  }

  //------------------------------------------------------------------------------
  // The vote for the current term should never change
  //------------------------------------------------------------------------------

  if(term == currentTerm && !votedFor.empty()) {
    return false;
  }

  //------------------------------------------------------------------------------
  // Just in case we crash in the middle, make sure votedFor becomes invalid first
  //------------------------------------------------------------------------------

  store.set_or_die("RAFT_VOTED_FOR", RaftState::BLOCKED_VOTE.toString());
  store.set_or_die("RAFT_CURRENT_TERM", std::to_string(term));
  store.set_or_die("RAFT_VOTED_FOR", vote.toString());

  currentTerm = term;
  votedFor = vote;
  return true;
}

void RaftJournal::setLastApplied(LogIndex index) {
  std::lock_guard<std::mutex> lock(lastAppliedMutex);

  if(index < lastApplied) {
    throw FatalException(SSTR("attempted to reduce lastApplied, from " << lastApplied << " to " << index));
  }

  if(logSize <= index) {
    throw FatalException(SSTR("attempted to set lastApplied to non-existent entry. index: " << index << ", logSize " << logSize));
  }

  store.set_or_die("RAFT_LAST_APPLIED", std::to_string(index));\
  lastApplied = index;
}

bool RaftJournal::append(LogIndex index, RaftTerm term, const RedisRequest &req) {
  std::lock_guard<std::mutex> lock(contentMutex);

  if(index != logSize) {
    qdb_warn("attempted to insert journal entry at an invalid position. index = " << index << ", logSize = " << logSize);
    return false;
  }

  if(term > currentTerm) {
    qdb_warn("attempted to insert journal entry with a higher term than the current one: " << term << " vs " << currentTerm);
    return false;
  }

  if(term < termOfLastEntry) {
    qdb_warn("attempted to insert journal entry with lower term " << term << ", while last one is " << termOfLastEntry);
    return false;
  }

  rawAppend(index, term, req);
  setLogSize(logSize+1);

  termOfLastEntry = term;
  logUpdated.notify_all();
  return true;
}

void RaftJournal::rawAppend(LogIndex index, RaftTerm term, const RedisRequest &cmd) {
  store.set_or_die(SSTR("RAFT_ENTRY_" << index), serializeRedisRequest(term, cmd));
}

void RaftJournal::setLogSize(LogIndex index) {
  if(index <= lastApplied) {
    throw FatalException(SSTR("Attempted to remove applied entry by setting logSize to " << index << " while lastApplied = " << lastApplied));
  }

  store.set_or_die("RAFT_LOG_SIZE", std::to_string(index));
  logSize = index;
}

RaftServer RaftJournal::getVotedFor() {
  std::lock_guard<std::mutex> lock(votedForMutex);
  return votedFor;
}

std::vector<RaftServer> RaftJournal::getNodes() {
  std::lock_guard<std::mutex> lock(nodesMutex);
  return nodes;
}

void RaftJournal::setNodes(const std::vector<RaftServer> &newNodes) {
  std::lock_guard<std::mutex> lock(nodesMutex);

  store.set_or_die("RAFT_NODES", serializeNodes(newNodes));
  nodes = newNodes;
}

void RaftJournal::setObservers(const std::vector<RaftServer> &obs) {
  std::lock_guard<std::mutex> lock(observersMutex);

  store.set_or_die("RAFT_OBSERVERS", serializeNodes(obs));
  observers = obs;
}

std::vector<RaftServer> RaftJournal::getObservers() {
  std::lock_guard<std::mutex> lock(observersMutex);
  return observers;
}

void RaftJournal::notifyWaitingThreads() {
  logUpdated.notify_all();
}

void RaftJournal::waitForUpdates(LogIndex currentSize, const std::chrono::milliseconds &timeout) {
  std::unique_lock<std::mutex> lock(contentMutex);

  // race, there's an update already
  if(currentSize < logSize) return;

  logUpdated.wait_for(lock, timeout);
}

bool RaftJournal::removeEntries(LogIndex from) {
  std::unique_lock<std::mutex> lock(contentMutex);
  if(logSize <= from) return false;

  if(from <= lastApplied) qdb_throw("attempted to remove committed entries. lastApplied: " << lastApplied << ", from: " << from);
  qdb_warn("Removing inconsistent log entries, [" << from << "," << logSize-1 << "]");

  for(LogIndex i = from; i < logSize; i++) {
    rocksdb::Status st = store.del(SSTR("RAFT_ENTRY_" << i));
    if(!st.ok()) qdb_critical("Error when deleting RAFT_ENTRY_" << i << ": " << st.ToString());
  }

  fetch_or_die(from-1, termOfLastEntry);
  this->setLogSize(from);
  return true;
}

bool RaftJournal::matchEntries(LogIndex index, RaftTerm term) {
  std::unique_lock<std::mutex> lock(contentMutex);

  if(logSize <= index) {
    return false;
  }

  RaftTerm tr;
  rocksdb::Status status = this->fetch(index, tr);

  if(!status.ok() && !status.IsNotFound()) {
    qdb_throw("rocksdb error: " << status.ToString());
  }

  return status.ok() && tr == term;
}

//------------------------------------------------------------------------------
// Log entry fetch operations
//------------------------------------------------------------------------------

rocksdb::Status RaftJournal::fetch(LogIndex index, RaftTerm &term, RedisRequest &cmd) {
  std::string data;
  rocksdb::Status st = store.get(SSTR("RAFT_ENTRY_" << index), data);
  if(!st.ok()) return st;

  deserializeRedisRequest(data, term, cmd);
  return st;
}

rocksdb::Status RaftJournal::fetch(LogIndex index, RaftTerm &term) {
  RedisRequest unused;
  return fetch(index, term, unused);
}

void RaftJournal::fetch_or_die(LogIndex index, RaftTerm &term, RedisRequest &cmd) {
  rocksdb::Status st = fetch(index, term, cmd);
  if(!st.ok()) {
    throw FatalException(SSTR("unable to fetch entry with index " << index));
  }
}

void RaftJournal::fetch_or_die(LogIndex index, RaftTerm &term) {
  rocksdb::Status st = fetch(index, term);
  if(!st.ok()) {
    qdb_throw("unable to fetch entry with index " << index);
  }
}
