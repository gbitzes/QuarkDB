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

#include "../RocksDB.hh"
#include <mutex>
#include <condition_variable>
#include "RaftCommon.hh"

namespace quarkdb {

class RaftJournal {
public:
  static void ObliterateAndReinitializeJournal(const std::string &path, RaftClusterID clusterID, std::vector<RaftServer> nodes);
  static void ObliterateAndReinitializeJournal(RocksDB &store, RaftClusterID clusterID, std::vector<RaftServer> nodes);

  // opens an existing journal
  RaftJournal(const std::string &path);

  // re-initializes a journal, obliterates the contents of the old one, if it exists
  RaftJournal(const std::string &path, RaftClusterID clusterID, const std::vector<RaftServer> &nodes);

  // should never have to be called during normal operation, only in the tests
  // assumes there's no other concurrent access to the journal
  void obliterate(RaftClusterID clusterID, const std::vector<RaftServer> &nodes);
  void initialize();

  bool setCurrentTerm(RaftTerm term, RaftServer vote);
  void setLastApplied(LogIndex index);
  void setNodes(const std::vector<RaftServer> &newNodes);
  void setObservers(const std::vector<RaftServer> &obs);

  RaftTerm getCurrentTerm() const { return currentTerm; }
  LogIndex getLogSize() const { return logSize; }
  RaftClusterID getClusterID() const { return clusterID; }
  LogIndex getLastApplied() const { return lastApplied; }
  std::vector<RaftServer> getNodes();
  RaftServer getVotedFor();
  std::vector<RaftServer> getObservers();

  bool append(LogIndex index, RaftTerm term, const RedisRequest &req);
  rocksdb::Status fetch(LogIndex index, RaftEntry &entry);
  rocksdb::Status fetch(LogIndex index, RaftTerm &term);
  void fetch_or_die(LogIndex index, RaftEntry &entry);
  void fetch_or_die(LogIndex index, RaftTerm &term);

  bool matchEntries(LogIndex index, RaftTerm term);
  bool removeEntries(LogIndex start);
  LogIndex compareEntries(LogIndex start, const std::vector<RaftEntry> entries);

  void waitForUpdates(LogIndex currentSize, const std::chrono::milliseconds &timeout);
  void notifyWaitingThreads();

  rocksdb::Status checkpoint(const std::string &path);
private:
  RocksDB store;

  //----------------------------------------------------------------------------
  // Cached values, always backed to stable storage
  //----------------------------------------------------------------------------

  std::atomic<RaftTerm> currentTerm;
  std::atomic<LogIndex> lastApplied;
  std::atomic<LogIndex> logSize;
  std::vector<RaftServer> nodes;
  std::vector<RaftServer> observers;
  RaftServer votedFor;
  RaftClusterID clusterID;

  std::mutex currentTermMutex;
  std::mutex lastAppliedMutex;
  std::mutex contentMutex;
  std::mutex nodesMutex;
  std::mutex observersMutex;
  std::mutex votedForMutex;

  std::condition_variable logUpdated;

  //----------------------------------------------------------------------------
  // Transient values, can always be inferred from stable storage
  //----------------------------------------------------------------------------

  RaftTerm termOfLastEntry;

  //----------------------------------------------------------------------------
  // Helper functions
  //----------------------------------------------------------------------------

  void rawAppend(LogIndex index, RaftTerm term, const RedisRequest &cmd);
  void setLogSize(LogIndex index);
};

}

#endif
