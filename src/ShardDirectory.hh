// ----------------------------------------------------------------------
// File: ShardDirectory.hh
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

#ifndef __QUARKDB_SHARD_DIRECTORY_H__
#define __QUARKDB_SHARD_DIRECTORY_H__

#include "Common.hh"

namespace quarkdb {

class StateMachine; class RaftJournal;

using ShardID = std::string;

// Manages a shard directory on the physical file system.
// Keeps ownership of StateMachine and RaftJournal - initialized lazily.
class ShardDirectory {
public:
  ShardDirectory(const std::string &path);
  ~ShardDirectory();

  StateMachine *getStateMachine();
  RaftJournal *getRaftJournal();

  // Reset the contents of both the state machine and the raft journal.
  // Physical paths remain the same.
  void obliterate(RaftClusterID clusterID, const std::vector<RaftServer> &nodes);

  // Create a standalone shard.
  static ShardDirectory* create(const std::string &path, RaftClusterID clusterID, ShardID shardID);

  // Create a consensus shard.
  static ShardDirectory* create(const std::string &path, RaftClusterID clusterID, ShardID shardID, const std::vector<RaftServer> &nodes);

private:
  std::string path;
  ShardID shardID;

  StateMachine *smptr = nullptr;
  RaftJournal *journalptr = nullptr;

  std::string stateMachinePath();
  std::string raftJournalPath();
  std::string currentPath();

  static void initializeDirectory(const std::string &path, RaftClusterID clusterID, ShardID shardID);
};



}


#endif
