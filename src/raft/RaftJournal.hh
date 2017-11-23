// ----------------------------------------------------------------------
// File: RaftJournal.hh
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

#ifndef __QUARKDB_RAFT_JOURNAL_H__
#define __QUARKDB_RAFT_JOURNAL_H__

#include <rocksdb/db.h>

#include <mutex>
#include <condition_variable>
#include "RaftCommon.hh"
#include "RaftMembers.hh"

namespace quarkdb {

class RaftJournal {
public:
  static void ObliterateAndReinitializeJournal(const std::string &path, RaftClusterID clusterID, std::vector<RaftServer> nodes);

  // opens an existing journal
  RaftJournal(const std::string &path);

  // re-initializes a journal, obliterates the contents of the old one, if it exists
  RaftJournal(const std::string &path, RaftClusterID clusterID, const std::vector<RaftServer> &nodes);
  ~RaftJournal();

  // should never have to be called during normal operation, only in the tests
  // assumes there's no other concurrent access to the journal
  void obliterate(RaftClusterID clusterID, const std::vector<RaftServer> &nodes);
  void initialize();

  bool setCurrentTerm(RaftTerm term, RaftServer vote);
  bool setCommitIndex(LogIndex index);

  RaftTerm getCurrentTerm() const { return currentTerm; }
  LogIndex getLogSize() const { return logSize; }
  LogIndex getLogStart() const { return logStart; }
  RaftClusterID getClusterID() const { return clusterID; }
  LogIndex getCommitIndex() const { return commitIndex; }
  std::vector<RaftServer> getNodes();
  RaftServer getVotedFor();

  LogIndex getEpoch() const { return membershipEpoch; }
  RaftMembership getMembership();

  bool append(LogIndex index, const RaftEntry &entry);
  rocksdb::Status fetch(LogIndex index, RaftEntry &entry);
  rocksdb::Status fetch(LogIndex index, RaftTerm &term);
  rocksdb::Status fetch(LogIndex index, RaftSerializedEntry &data);

  void fetch_or_die(LogIndex index, RaftEntry &entry);
  void fetch_or_die(LogIndex index, RaftTerm &term);

  bool matchEntries(LogIndex index, RaftTerm term);
  bool removeEntries(LogIndex start);
  LogIndex compareEntries(LogIndex start, const std::vector<RaftEntry> entries);

  void waitForUpdates(LogIndex currentSize, const std::chrono::milliseconds &timeout);
  bool waitForCommits(const LogIndex currentCommit);
  void notifyWaitingThreads();

  std::string getDBPath() { return dbPath; }
  rocksdb::Status checkpoint(const std::string &path);
  void trimUntil(LogIndex newLogStart);

  bool addObserver(RaftTerm term, const RaftServer &observer, std::string &err);
  bool promoteObserver(RaftTerm term, const RaftServer &obserer, std::string &err);
  bool removeMember(RaftTerm term, const RaftServer &member, std::string &err);

  bool appendLeadershipMarker(LogIndex index, RaftTerm term, const RaftServer &leader);
private:
  void openDB(const std::string &path);

  rocksdb::DB* db = nullptr;
  std::string dbPath;

  using IteratorPtr = std::unique_ptr<rocksdb::Iterator>;

  //----------------------------------------------------------------------------
  // Cached values, always backed to stable storage
  //----------------------------------------------------------------------------

  std::atomic<RaftTerm> currentTerm;
  std::atomic<LogIndex> commitIndex;
  std::atomic<LogIndex> logSize;
  std::atomic<LogIndex> logStart;
  std::atomic<LogIndex> membershipEpoch;
  RaftMembers members;
  RaftServer votedFor;
  RaftClusterID clusterID;

  std::mutex currentTermMutex;
  std::mutex lastAppliedMutex;
  std::mutex commitIndexMutex;
  std::mutex contentMutex;
  std::mutex membersMutex;
  std::mutex votedForMutex;

  std::condition_variable commitNotifier;
  std::condition_variable logUpdated;

  //----------------------------------------------------------------------------
  // Utility functions for write batches
  //----------------------------------------------------------------------------

  void commitBatch(rocksdb::WriteBatch &batch, LogIndex index = -1);

  //----------------------------------------------------------------------------
  // Transient values, can always be inferred from stable storage
  //----------------------------------------------------------------------------

  RaftTerm termOfLastEntry;

  //----------------------------------------------------------------------------
  // Helper functions
  //----------------------------------------------------------------------------

  RaftMembers getMembers();
  bool membershipUpdate(RaftTerm term, const RaftMembers &newMembers, std::string &err);
  bool appendNoLock(LogIndex index, const RaftEntry &entry);

  void set_or_die(const std::string &key, const std::string &value);
  void set_int_or_die(const std::string &key, int64_t value);
  std::string get_or_die(const std::string &key);
  int64_t get_int_or_die(const std::string &key);
};

}

#endif
