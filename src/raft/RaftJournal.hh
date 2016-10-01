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

namespace quarkdb {

class RaftJournal {
public:
  static void ObliterateAndReinitializeJournal(const std::string &path, RaftClusterID clusterID, std::vector<RaftServer> nodes);
  RaftJournal(const std::string &path);

  bool setCurrentTerm(RaftTerm term, RaftServer vote);
  void setLastApplied(LogIndex index);
  void setNodes(const std::vector<RaftServer> &newNodes);
  void setObservers(const std::vector<RaftServer> &obs);

  RaftTerm getCurrentTerm() { return currentTerm; }
  LogIndex getLogSize() { return logSize; }
  RaftClusterID getClusterID() { return clusterID; }
  LogIndex getLastApplied() { return lastApplied; }
  std::vector<RaftServer> getNodes();
  RaftServer getVotedFor();
  std::vector<RaftServer> getObservers();

  LogIndex append(RaftTerm term, RedisRequest &req);
  rocksdb::Status fetch(LogIndex index, RaftTerm &term, RedisRequest &cmd);
  rocksdb::Status fetch(LogIndex index, RaftTerm &term);
  void fetch_or_die(LogIndex index, RaftTerm &term, RedisRequest &cmd);
  void fetch_or_die(LogIndex index, RaftTerm &term);
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

  //----------------------------------------------------------------------------
  // Transient values, can always be inferred from stable storage
  //----------------------------------------------------------------------------

  RaftTerm termOfLastEntry;

  //----------------------------------------------------------------------------
  // Helper functions
  //----------------------------------------------------------------------------

  void rawAppend(RaftTerm term, LogIndex index, const RedisRequest &cmd);
  void setLogSize(LogIndex index);
};

}

#endif
