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
#include "utils/FileDescriptor.hh"
#include "raft/RaftJournal.hh"

using namespace quarkdb;

ShardSnapshot::ShardSnapshot(const std::string &path_)
: path(path_) { }

ShardSnapshot::~ShardSnapshot() {
  system(SSTR("rm -rf " << path).c_str());
}

std::string ShardSnapshot::getPath() {
  return path;
}

std::string ShardDirectory::resilveringHistoryPath() {
  return pathJoin(path, "RESILVERING-HISTORY");
}

void ShardDirectory::parseResilveringHistory() {
  std::string historyPath(resilveringHistoryPath()), tmp;
  if(!readFile(historyPath, tmp)) {
    qdb_throw("Unable to read resilvering history from '" << historyPath << "'");
  }

  if(!ResilveringHistory::deserialize(tmp, resilveringHistory)) {
    qdb_throw("Unable to parse resilvering history from " << q(historyPath));
  }
}

void ShardDirectory::storeResilveringHistory() {
  write_file_or_die(resilveringHistoryPath(), resilveringHistory.serialize());
}

ShardDirectory::ShardDirectory(const std::string &initpath)
: path(initpath) {

  std::string err;
  if(!directoryExists(path, err)) {
    qdb_throw("Cannot initialize shard directory at '" << path << "': " << err);
  }

  std::string idPath(pathJoin(path, "SHARD-ID"));
  if(!readFile(idPath, shardID)) {
    qdb_throw("Unable to read shard id from '" << idPath << "'");
  }

  parseResilveringHistory();
}

ShardDirectory::~ShardDirectory() {
  detach();
}

void ShardDirectory::detach() {
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

std::string ShardDirectory::currentPath() {
  return pathJoin(path, "current");
}

std::string ShardDirectory::stateMachinePath() {
  return pathJoin(currentPath(), "state-machine");
}

std::string ShardDirectory::raftJournalPath() {
  return pathJoin(currentPath(), "raft-journal");
}

void ShardDirectory::obliterate(RaftClusterID clusterID, const std::vector<RaftServer> &nodes) {
  getStateMachine()->reset();

  if(!journalptr) {
    journalptr = new RaftJournal(raftJournalPath(), clusterID, nodes);
  }
  else {
    getRaftJournal()->obliterate(clusterID, nodes);
  }

  resilveringHistory.clear();
  resilveringHistory.append(ResilveringEvent("GENESIS", time(NULL)));
  storeResilveringHistory();
}

void ShardDirectory::initializeDirectory(const std::string &path, RaftClusterID clusterID, ShardID shardID) {
  std::string err;
  if(directoryExists(path, err)) {
    qdb_throw("Cannot initialize shard directory for '" << shardID << "', path already exists: " << path);
  }

  mkpath_or_die(path + "/", 0755);
  write_file_or_die(pathJoin(path, "SHARD-ID"), shardID);
  mkpath_or_die(pathJoin(path, "current") + "/", 0755);

  ResilveringHistory history;
  history.append(ResilveringEvent("GENESIS", time(NULL)));
  write_file_or_die(pathJoin(path, "RESILVERING-HISTORY"), history.serialize());
}

ShardDirectory* ShardDirectory::create(const std::string &path, RaftClusterID clusterID, ShardID shardID) {
  initializeDirectory(path, clusterID, shardID);
  return new ShardDirectory(path);
}

ShardDirectory* ShardDirectory::create(const std::string &path, RaftClusterID clusterID, ShardID shardID, const std::vector<RaftServer> &nodes) {
  initializeDirectory(path, clusterID, shardID);

  ShardDirectory *shardDirectory = new ShardDirectory(path);
  shardDirectory->obliterate(clusterID, nodes);
  return shardDirectory;
}

// Before calling this function, journal trimming should have been turned off!
std::unique_ptr<ShardSnapshot> ShardDirectory::takeSnapshot(const SnapshotID &id, std::string &err) {
  std::string snapshotDirectory = getTempSnapshot(id);

  if(!mkpath(snapshotDirectory + "/", 0755, err)) {
    qdb_critical(err);
    return nullptr;
  }

  std::string journalCheckpoint = pathJoin(snapshotDirectory, "raft-journal");
  rocksdb::Status st = getRaftJournal()->checkpoint(journalCheckpoint);
  if(!st.ok()) {
    qdb_critical("cannot create journal checkpoint in " << journalCheckpoint << ": " << st.ToString());
    return nullptr;
  }

  std::string smCheckpoint = pathJoin(snapshotDirectory, "state-machine");
  st = getStateMachine()->checkpoint(smCheckpoint);
  if(!st.ok()) {
    qdb_critical("cannot create state machine checkpoint in " << smCheckpoint << ": " << st.ToString());
    return nullptr;
  }

  return  std::unique_ptr<ShardSnapshot>(new ShardSnapshot(snapshotDirectory));
}

bool ShardDirectory::resilveringStart(const ResilveringEventID &id, std::string &err) {
  if(!mkpath(getResilveringArena(id) + "/", 0755, err)) {
    err = SSTR("Unable to create resilvering-arena for '" << id << "'");
    qdb_critical(err);
    return false;
  }

  return true;
}

bool ShardDirectory::resilveringCopy(const ResilveringEventID &id, const std::string &filename, const std::string &contents, std::string &err) {
  std::string targetPath = pathJoin(getResilveringArena(id), filename);

  if(!mkpath(targetPath, 0755, err)) {
    goto error;
  }

  if(!write_file(targetPath, contents, err)) {
    goto error;
  }

  return true;

error:
  qdb_critical("error during resilveringCopy: " << err);
  return false;
}

// When calling this function, we assume caller has released any references
// to the journal and state machine!
bool ShardDirectory::resilveringFinish(const ResilveringEventID &id, std::string &err) {
  std::string resilveringArena = getResilveringArena(id);
  if(!directoryExists(resilveringArena, err)) {
    return false;
  }

  detach();

  qdb_event("Finalizing resilvering, id '" << id << "'.");

  std::string supplanted = getSupplanted(id);
  mkpath_or_die(supplanted, 0755);

  std::string source = currentPath();
  std::string destination = supplanted;
  rename_directory_or_die(source, destination);

  source = resilveringArena;
  destination = currentPath();
  rename_directory_or_die(source, destination);

  // By some kind of miracle, we have survived up to this point. Attach!
  getStateMachine();
  getRaftJournal();

  // Store the resilvering event into the history.
  resilveringHistory.append(ResilveringEvent(id, time(NULL)));
  storeResilveringHistory();

  qdb_event("Resilvering '" << id << "'" << " has been successful!");
  return true;
}

std::string ShardDirectory::getSupplanted(const ResilveringEventID &id) {
  return pathJoin(pathJoin(path, "supplanted"), id);
}

std::string ShardDirectory::getResilveringArena(const ResilveringEventID &id) {
  return pathJoin(pathJoin(path, "resilvering-arena"), id);
}

std::string ShardDirectory::getTempSnapshot(const SnapshotID &id) {
  return pathJoin(pathJoin(path, "temp-snapshots"), id);
}

const ResilveringHistory& ShardDirectory::getResilveringHistory() const {
  return resilveringHistory;
}
