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

#include <algorithm>
#include "RaftJournal.hh"
#include "RaftMembers.hh"
#include "../Common.hh"
#include "../Utils.hh"
#include "RaftState.hh"
#include <rocksdb/utilities/checkpoint.h>

using namespace quarkdb;
#define THROW_ON_ERROR(stmt) { rocksdb::Status st2 = stmt; if(!st2.ok()) qdb_throw(st2.ToString()); }

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

static std::string encodeEntryKey(LogIndex index) {
  return SSTR("ENTRY_" << intToBinaryString(index));
}

//------------------------------------------------------------------------------
// RaftJournal
//------------------------------------------------------------------------------

void RaftJournal::ObliterateAndReinitializeJournal(const std::string &path, RaftClusterID clusterID, std::vector<RaftServer> nodes) {
  RaftJournal journal(path, clusterID, nodes);
}

void RaftJournal::obliterate(RaftClusterID newClusterID, const std::vector<RaftServer> &newNodes) {
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    db->Delete(rocksdb::WriteOptions(), iter->key().ToString());
  }

  this->set_int_or_die("RAFT_CURRENT_TERM", 0);
  this->set_int_or_die("RAFT_LOG_SIZE", 1);
  this->set_int_or_die("RAFT_LOG_START", 0);
  this->set_or_die("RAFT_CLUSTER_ID", newClusterID);
  this->set_or_die("RAFT_VOTED_FOR", "");
  this->set_int_or_die("RAFT_COMMIT_INDEX", 0);

  RaftMembers newMembers(newNodes, {});
  this->set_or_die("RAFT_MEMBERS", newMembers.toString());
  this->set_int_or_die("RAFT_MEMBERSHIP_EPOCH", 0);

  RedisRequest req { "JOURNAL_UPDATE_MEMBERS", newMembers.toString(), newClusterID };
  this->set_or_die(encodeEntryKey(0), serializeRedisRequest(0, req));

  initialize();
}

void RaftJournal::initialize() {
  currentTerm = this->get_int_or_die("RAFT_CURRENT_TERM");
  logSize = this->get_int_or_die("RAFT_LOG_SIZE");
  logStart = this->get_int_or_die("RAFT_LOG_START");
  clusterID = this->get_or_die("RAFT_CLUSTER_ID");
  commitIndex = this->get_int_or_die("RAFT_COMMIT_INDEX");
  std::string vote = this->get_or_die("RAFT_VOTED_FOR");
  this->fetch_or_die(logSize-1, termOfLastEntry);

  membershipEpoch = this->get_int_or_die("RAFT_MEMBERSHIP_EPOCH");
  members = RaftMembers(this->get_or_die("RAFT_MEMBERS"));

  if(!vote.empty() && !parseServer(vote, votedFor)) {
    qdb_throw("journal corruption, cannot parse RAFT_VOTED_FOR: " << vote);
  }
}

void RaftJournal::openDB(const std::string &path) {
  qdb_info("Opening journal database " << quotes(path));
  dbPath = path;

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::TransactionDB::Open(options, rocksdb::TransactionDBOptions(), path, &transactionDB);
  if(!status.ok()) qdb_throw("Error while opening journal in " << path << ":" << status.ToString());

  db = transactionDB->GetBaseDB();
}

RaftJournal::RaftJournal(const std::string &filename, RaftClusterID clusterID, const std::vector<RaftServer> &nodes) {
  openDB(filename);
  obliterate(clusterID, nodes);
}

RaftJournal::~RaftJournal() {
  qdb_info("Closing journal database " << quotes(dbPath));

  if(transactionDB) {
    delete transactionDB;
    transactionDB = nullptr;
    db = nullptr;
  }
}

RaftJournal::RaftJournal(const std::string &filename) {
  openDB(filename);
  initialize();
}

bool RaftJournal::setCurrentTerm(RaftTerm term, RaftServer vote) {
  std::lock_guard<std::mutex> lock(currentTermMutex);

  //----------------------------------------------------------------------------
  // Terms should never go back in time
  //----------------------------------------------------------------------------

  if(term < currentTerm) {
    return false;
  }

  //----------------------------------------------------------------------------
  // The vote for the current term should never change
  //----------------------------------------------------------------------------

  if(term == currentTerm && !votedFor.empty()) {
    return false;
  }

  //----------------------------------------------------------------------------
  // Atomically update currentTerm and votedFor
  //----------------------------------------------------------------------------

  TransactionPtr tx(startTransaction());
  THROW_ON_ERROR(tx->Put("RAFT_CURRENT_TERM", intToBinaryString(term)));
  THROW_ON_ERROR(tx->Put("RAFT_VOTED_FOR", vote.toString()));
  commitTransaction(tx);

  currentTerm = term;
  votedFor = vote;
  return true;
}

bool RaftJournal::setCommitIndex(LogIndex newIndex) {
  std::lock_guard<std::mutex> lock(commitIndexMutex);
  if(newIndex < commitIndex) {
    qdb_critical("attempted to set commit index in the past, from " << commitIndex << " ==> " << newIndex);
    return false;
  }

  if(logSize <= newIndex) {
    qdb_throw("attempted to mark as committed a non-existing entry. Journal size: " << logSize << ", new index: " << newIndex);
  }

  if(commitIndex < newIndex) {
    this->set_int_or_die("RAFT_COMMIT_INDEX", newIndex);
    commitIndex = newIndex;
    commitNotifier.notify_all();
  }
  return true;
}

bool RaftJournal::waitForCommits(const LogIndex currentCommit) {
  std::unique_lock<std::mutex> lock(commitIndexMutex);
  if(currentCommit < commitIndex) return true;

  commitNotifier.wait(lock);
  return true;
}

RaftJournal::TransactionPtr RaftJournal::startTransaction() {
  return TransactionPtr(transactionDB->BeginTransaction(rocksdb::WriteOptions()));
}

void RaftJournal::commitTransaction(TransactionPtr &tx, LogIndex index) {
  if(index >= 0 && index <= commitIndex) {
    qdb_throw("Attempted to remove committed entries by setting logSize to " << index << " while commitIndex = " << commitIndex);
  }

  if(index >= 0 && index != logSize) {
    THROW_ON_ERROR(tx->Put("RAFT_LOG_SIZE", intToBinaryString(index)));
  }

  rocksdb::Status st = tx->Commit();
  if(!st.ok()) qdb_throw("unable to commit journal transaction: " << st.ToString());
  if(index >= 0) logSize = index;
}

RaftMembers RaftJournal::getMembers() {
  std::lock_guard<std::mutex> lock(membersMutex);
  return members;
}

RaftMembership RaftJournal::getMembership() {
  std::lock_guard<std::mutex> lock(membersMutex);
  return {members.nodes, members.observers, membershipEpoch};
}

bool RaftJournal::membershipUpdate(RaftTerm term, const RaftMembers &newMembers, std::string &err) {
  std::lock_guard<std::mutex> lock(contentMutex);

  if(commitIndex < membershipEpoch) {
    err = SSTR("The current membership epoch has not been committed yet: " << membershipEpoch);
    return false;
  }

  RedisRequest req = {"JOURNAL_UPDATE_MEMBERS", newMembers.toString(), clusterID };
  return appendNoLock(logSize, term, req);
}

bool RaftJournal::addObserver(RaftTerm term, const RaftServer &observer, std::string &err) {
  RaftMembers newMembers = getMembers();
  if(!newMembers.addObserver(observer, err)) return false;
  return membershipUpdate(term, newMembers, err);
}

bool RaftJournal::removeMember(RaftTerm term, const RaftServer &member, std::string &err) {
  RaftMembers newMembers = getMembers();
  if(!newMembers.removeMember(member, err)) return false;
  return membershipUpdate(term, newMembers, err);
}

bool RaftJournal::promoteObserver(RaftTerm term, const RaftServer &observer, std::string &err) {
  RaftMembers newMembers = getMembers();
  if(!newMembers.promoteObserver(observer, err)) return false;
  return membershipUpdate(term, newMembers, err);
}

bool RaftJournal::appendNoLock(LogIndex index, RaftTerm term, const RedisRequest &req) {
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

  TransactionPtr tx(startTransaction());

  if(req[0] == "JOURNAL_UPDATE_MEMBERS") {
    if(req.size() != 3) qdb_throw("Journal corruption, invalid journal_update_members: " << req);

    //--------------------------------------------------------------------------
    // Special case for membership updates
    // We don't wait until the entry is committed, and it takes effect
    // immediatelly.
    // The commit applier will ignore such entries, and apply a no-op to the
    // state machine.
    //--------------------------------------------------------------------------

    if(req[2] == clusterID) {
      THROW_ON_ERROR(tx->Put("RAFT_MEMBERS", req[1]));
      THROW_ON_ERROR(tx->Put("RAFT_MEMBERSHIP_EPOCH", intToBinaryString(index)));

      THROW_ON_ERROR(tx->Put("RAFT_PREVIOUS_MEMBERS", members.toString()));
      THROW_ON_ERROR(tx->Put("RAFT_PREVIOUS_MEMBERSHIP_EPOCH", intToBinaryString(membershipEpoch)));

      qdb_event("Transitioning into a new membership epoch: " << membershipEpoch << " => " << index
      << ". Old members: " << members.toString() << ", new members: " << req[1]);

      std::lock_guard<std::mutex> lock(membersMutex);
      members = RaftMembers(req[1]);
      membershipEpoch = index;
    }
    else {
      qdb_critical("Received request for membership update " << req << ", but the clusterIDs do not match - mine is " << clusterID
      << ". THE MEMBERSHIP UPDATE ENTRY WILL BE IGNORED. Something is either corrupted or you force-reconfigured " <<
      " the nodes recently - if it's the latter, this message is nothing to worry about.");
    }
  }

  THROW_ON_ERROR(tx->Put(encodeEntryKey(index), serializeRedisRequest(term, req)));
  commitTransaction(tx, index+1);

  termOfLastEntry = term;
  logUpdated.notify_all();
  return true;
}

bool RaftJournal::append(LogIndex index, RaftTerm term, const RedisRequest &req) {
  std::lock_guard<std::mutex> lock(contentMutex);
  return appendNoLock(index, term, req);
}

void RaftJournal::trimUntil(LogIndex newLogStart) {
  // no locking - trimmed entries should be so old
  // that they are not being accessed anymore

  if(newLogStart <= logStart) return; // no entries to trim
  if(logSize < newLogStart) qdb_throw("attempted to trim a journal past its end. logSize: " << logSize << ", new log start: " << newLogStart);
  if(commitIndex < newLogStart) qdb_throw("attempted to trim non-committed entries. commitIndex: " << commitIndex << ", new log start: " << newLogStart);

  qdb_info("Trimming raft journal from #" << logStart << " until #" << newLogStart);
  TransactionPtr tx(startTransaction());

  for(LogIndex i = logStart; i < newLogStart; i++) {
    rocksdb::Status st = tx->Delete(encodeEntryKey(i));
    if(!st.ok()) qdb_throw("Error when trimming journal, cannot delete entry " << i << ": " << st.ToString());
  }

  THROW_ON_ERROR(tx->Put("RAFT_LOG_START", intToBinaryString(newLogStart)));
  commitTransaction(tx);
  logStart = newLogStart;
}

RaftServer RaftJournal::getVotedFor() {
  std::lock_guard<std::mutex> lock(votedForMutex);
  return votedFor;
}

std::vector<RaftServer> RaftJournal::getNodes() {
  return getMembership().nodes;
}

void RaftJournal::notifyWaitingThreads() {
  logUpdated.notify_all();
  commitNotifier.notify_all();
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

  if(from <= commitIndex) qdb_throw("attempted to remove committed entries. commitIndex: " << commitIndex << ", from: " << from);
  qdb_warn("Removing inconsistent log entries: [" << from << "," << logSize-1 << "]");

  TransactionPtr tx(startTransaction());
  for(LogIndex i = from; i < logSize; i++) {
    rocksdb::Status st = tx->Delete(encodeEntryKey(i));
    if(!st.ok()) qdb_critical("Error when deleting entry " << i << ": " << st.ToString());
  }

  //----------------------------------------------------------------------------
  // Membership epochs take effect immediatelly, without waiting for the entries
  // to be committed. (as per the Raft PhD thesis)
  // This means that an uncommitted membership epoch can be theoretically rolled
  // back.
  // This should be extremely uncommon, so we log a critical message.
  //----------------------------------------------------------------------------

  if(from <= membershipEpoch) {
    std::lock_guard<std::mutex> lock2(membersMutex);

    LogIndex previousMembershipEpoch = this->get_int_or_die("RAFT_PREVIOUS_MEMBERSHIP_EPOCH");
    std::string previousMembers = this->get_or_die("RAFT_PREVIOUS_MEMBERS");

    THROW_ON_ERROR(tx->Put("RAFT_MEMBERSHIP_EPOCH", intToBinaryString(previousMembershipEpoch)));
    THROW_ON_ERROR(tx->Put("RAFT_MEMBERS", previousMembers));

    qdb_critical("Rolling back an uncommitted membership epoch. Transitioning from " <<
    membershipEpoch << " => " << previousMembershipEpoch << ". Old members: " << members.toString() <<
    ", new members: " << previousMembers);

    members = RaftMembers(previousMembers);
    membershipEpoch = previousMembershipEpoch;
  }

  commitTransaction(tx, from);
  fetch_or_die(from-1, termOfLastEntry);
  return true;
}

// return the first entry which is not identical to the ones in the vector
LogIndex RaftJournal::compareEntries(LogIndex start, const std::vector<RaftEntry> entries) {
  std::unique_lock<std::mutex> lock(contentMutex);

  LogIndex endIndex = std::min(LogIndex(logSize), LogIndex(start+entries.size()));
  LogIndex startIndex = std::max(start, LogIndex(logStart));

  if(start != startIndex) {
    qdb_critical("Tried to compare entries which have already been trimmed.. will assume they contain no inconsistencies. logStart: " << logStart << ", asked to compare starting from: " << start);
  }

  for(LogIndex i = startIndex; i < endIndex; i++) {
    RaftEntry entry;
    fetch_or_die(i, entry);
    if(entries[i-start] != entry) {
      qdb_warn("Detected inconsistency for entry #" << i << ". Contents of my journal: " << entry << ". Contents of what the leader sent: " << entries[i-start]);
      return i;
    }
  }

  return endIndex;
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

rocksdb::Status RaftJournal::fetch(LogIndex index, RaftEntry &entry) {
  // we intentionally do not check logSize and logStart, so as to be able to
  // catch potential inconsistencies between the counters and what is
  // really contained in the journal

  std::string data;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), encodeEntryKey(index), &data);
  if(!st.ok()) return st;

  deserializeRedisRequest(data, entry.term, entry.request);
  return st;
}

rocksdb::Status RaftJournal::fetch(LogIndex index, RaftTerm &term) {
  RaftEntry entry;
  rocksdb::Status st = fetch(index, entry);
  term = entry.term;
  return st;
}

void RaftJournal::fetch_or_die(LogIndex index, RaftEntry &entry) {
  rocksdb::Status st = fetch(index, entry);
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

void RaftJournal::set_or_die(const std::string &key, const std::string &value) {
  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), key, value);
  if(!st.ok()) {
    qdb_throw("unable to set journal key " << key << ". Error: " << st.ToString());
  }
}

void RaftJournal::set_int_or_die(const std::string &key, int64_t value) {
  this->set_or_die(key, intToBinaryString(value));
}

int64_t RaftJournal::get_int_or_die(const std::string &key) {
  return binaryStringToInt(this->get_or_die(key).c_str());
}

std::string RaftJournal::get_or_die(const std::string &key) {
  std::string tmp;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), key, &tmp);
  if(!st.ok()) qdb_throw("error when getting journal key " << key << ": " << st.ToString());
  return tmp;
}

//------------------------------------------------------------------------------
// Checkpoint for online backup
//------------------------------------------------------------------------------

rocksdb::Status RaftJournal::checkpoint(const std::string &path) {
  rocksdb::Checkpoint *checkpoint = nullptr;
  rocksdb::Status st = rocksdb::Checkpoint::Create(db, &checkpoint);
  if(!st.ok()) return st;

  st = checkpoint->CreateCheckpoint(path);
  delete checkpoint;

  return st;
}
