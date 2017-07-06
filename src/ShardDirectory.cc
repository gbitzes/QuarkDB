// ----------------------------------------------------------------------
// File: ShardDirectory.cc
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

#include "utils/FileUtils.hh"
#include "ShardDirectory.hh"
#include "StateMachine.hh"
#include "raft/RaftJournal.hh"

using namespace quarkdb;

ShardDirectory::ShardDirectory(const std::string &initpath)
: path(initpath) {

  std::string err;
  if(!directoryExists(path, err)) {
    qdb_throw("Cannot initialize shard directory at '" << path << "': " << err);
  }
}

ShardDirectory::~ShardDirectory() {
  if(smptr) {
    delete smptr;
    smptr = nullptr;
  }

  if(journalptr) {
    delete journalptr;
    journalptr = nullptr;
  }
}

StateMachine* ShardDirectory::getStateMachine() {
  if(smptr) return smptr;

  smptr = new StateMachine(stateMachinePath());
  return smptr;
}

RaftJournal* ShardDirectory::getRaftJournal() {
  if(journalptr) return journalptr;

  std::string suberr;
  if(!directoryExists(raftJournalPath(), suberr)) {
    qdb_throw("Cannot open raft journal: " << suberr);
  }

  journalptr = new RaftJournal(raftJournalPath());
  return journalptr;
}

std::string ShardDirectory::stateMachinePath() {
  return pathJoin(path, "state-machine");
}

std::string ShardDirectory::raftJournalPath() {
  return pathJoin(path, "raft-journal");
}

void ShardDirectory::obliterate(RaftClusterID clusterID, const std::vector<RaftServer> &nodes) {
  getStateMachine()->reset();

  if(!journalptr) {
    journalptr = new RaftJournal(raftJournalPath(), clusterID, nodes);
  }
  else {
    getRaftJournal()->obliterate(clusterID, nodes);
  }
}
