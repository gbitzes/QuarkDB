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

#include "ShardDirectory.hh"
#include "utils/FileUtils.hh"
#include "StateMachine.hh"
#include "raft/RaftJournal.hh"

#include <sys/stat.h>

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

ShardDirectory::ShardDirectory(const std::string &initpath, Configuration config)
: path(initpath), configuration(config) {

  std::string err;
  if(!directoryExists(path, err)) {
    qdb_fatal("Cannot initialize shard directory at '" << path << "': " << err);
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

StateMachine* ShardDirectory::getStateMachineForBulkload() {
  qdb_assert(!smptr);
  smptr = new StateMachine(stateMachinePath(), false, true);
  return smptr;
}

StateMachine* ShardDirectory::getStateMachine() {
  if(smptr) return smptr;

  smptr = new StateMachine(stateMachinePath(), configuration.getWriteAheadLog());
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

//------------------------------------------------------------------------------
// Wipe out StateMachine contents.
//------------------------------------------------------------------------------
void ShardDirectory::wipeoutStateMachineContents() {
  if(smptr) {
    //--------------------------------------------------------------------------
    // We have the state machine open already.. wipe contents through reset
    //--------------------------------------------------------------------------
    getStateMachine()->reset();
  }
  else {
    //--------------------------------------------------------------------------
    // Not open, simply delete the entire folder
    //--------------------------------------------------------------------------
    qdb_assert(system(SSTR("rm -rf '" << stateMachinePath() << "'").c_str()) == 0);
  }
}

//------------------------------------------------------------------------------
// Initialize our StateMachine with the given source, if any.
// If no source is given, create a brand new one.
//------------------------------------------------------------------------------
void ShardDirectory::initializeStateMachine(std::unique_ptr<StateMachine> sm, LogIndex initialLastApplied) {
  if(!sm && initialLastApplied == 0) {
    //--------------------------------------------------------------------------
    // Simplest case: No seed machine, and starting from 0.
    // Just ensure SM is wiped out.
    //--------------------------------------------------------------------------
    wipeoutStateMachineContents();
    return;
  }

  if(!sm) {
    //--------------------------------------------------------------------------
    // No seed machine, but starting from non-zero.
    //--------------------------------------------------------------------------
    wipeoutStateMachineContents();
    getStateMachine()->forceResetLastApplied(initialLastApplied);
  }

  //----------------------------------------------------------------------------
  // We have to reset any old contents with those of the pre-existing seed
  // StateMachine. First, get the target filename..
  //----------------------------------------------------------------------------
  std::string sourceStateMahchine = sm->getPhysicalLocation();

  //----------------------------------------------------------------------------
  // Shut it down - we don't want to be moving files of a live SM..
  //----------------------------------------------------------------------------
  sm.reset();

  //----------------------------------------------------------------------------
  // Shut down and wipe out any existing, to-be-deleted SMs we own
  //----------------------------------------------------------------------------
  detach();
  wipeoutStateMachineContents();

  //----------------------------------------------------------------------------
  // Do the actual move
  //----------------------------------------------------------------------------
  qdb_assert(system(SSTR("mv " << quotes(sourceStateMahchine) << " " << quotes(stateMachinePath()) ).c_str()) == 0);

  //----------------------------------------------------------------------------
  // Force reset lastApplied.
  //----------------------------------------------------------------------------
  getStateMachine()->forceResetLastApplied(initialLastApplied);
}

void ShardDirectory::obliterate(RaftClusterID clusterID, const std::vector<RaftServer> &nodes, LogIndex startIndex, std::unique_ptr<StateMachine> sm) {
  bool hasSeedSM = (sm.get() != nullptr);
  initializeStateMachine(std::move(sm), startIndex);

  if(!journalptr) {
    journalptr = new RaftJournal(raftJournalPath(), clusterID, nodes, startIndex);
  }
  else {
    getRaftJournal()->obliterate(clusterID, nodes, startIndex);
  }

  resilveringHistory.clear();

  if(!hasSeedSM) {
    resilveringHistory.append(ResilveringEvent("GENESIS", time(NULL)));
  }
  else {
    resilveringHistory.append(ResilveringEvent(SSTR("GENESIS-FROM-EXISTING-SM-AT-INDEX:" << startIndex), time(NULL)));
  }

  storeResilveringHistory();
}

Status ShardDirectory::initializeDirectory(const std::string &path, RaftClusterID clusterID, ShardID shardID) {
  std::string err;
  if(directoryExists(path, err)) {
    return Status(EEXIST, SSTR("Cannot initialize shard directory for '" << shardID << "', path already exists: " << path));
  }

  mkpath_or_die(path + "/", 0755);
  write_file_or_die(pathJoin(path, "SHARD-ID"), shardID);
  mkpath_or_die(pathJoin(path, "current") + "/", 0755);

  ResilveringHistory history;
  history.append(ResilveringEvent("GENESIS", time(NULL)));
  write_file_or_die(pathJoin(path, "RESILVERING-HISTORY"), history.serialize());
  return Status();
}

ShardDirectory* ShardDirectory::create(const std::string &path, RaftClusterID clusterID, ShardID shardID, std::unique_ptr<StateMachine> sm, Status &st) {
  st = initializeDirectory(path, clusterID, shardID);
  if(!st.ok()) {
    return nullptr;
  }

  ShardDirectory *shardDirectory = new ShardDirectory(path);

  // Standalone shard, we start from LogIndex 0
  shardDirectory->initializeStateMachine(std::move(sm), 0);
  return new ShardDirectory(path);
}

ShardDirectory* ShardDirectory::create(const std::string &path, RaftClusterID clusterID, ShardID shardID, const std::vector<RaftServer> &nodes, LogIndex startIndex, std::unique_ptr<StateMachine> sm, Status &st) {
  st = initializeDirectory(path, clusterID, shardID);
  if(!st.ok()) {
    return nullptr;
  }


  ShardDirectory *shardDirectory = new ShardDirectory(path);
  shardDirectory->obliterate(clusterID, nodes, startIndex, std::move(sm) );
  return shardDirectory;
}

// Before calling this function, journal trimming should have been turned off!
std::unique_ptr<ShardSnapshot> ShardDirectory::takeSnapshot(const SnapshotID &id, std::string &err) {
  std::string snapshotDirectory = getTempSnapshot(id);

  if(!mkpath(snapshotDirectory + "/", 0755, err)) {
    qdb_critical(err);
    return nullptr;
  }

  std::string smCheckpoint = pathJoin(snapshotDirectory, "state-machine");
  rocksdb::Status st = getStateMachine()->checkpoint(smCheckpoint);
  if(!st.ok()) {
    qdb_critical("cannot create state machine checkpoint in " << smCheckpoint << ": " << st.ToString());
    return nullptr;
  }

  std::string journalCheckpoint = pathJoin(snapshotDirectory, "raft-journal");
  st = getRaftJournal()->checkpoint(journalCheckpoint);
  if(!st.ok()) {
    qdb_critical("cannot create journal checkpoint in " << journalCheckpoint << ": " << st.ToString());
    return nullptr;
  }

  return std::unique_ptr<ShardSnapshot>(new ShardSnapshot(snapshotDirectory));
}

bool ShardDirectory::resilveringStart(const ResilveringEventID &id, std::string &err) {
  if(!mkpath(getResilveringArena(id) + "/", 0755, err)) {
    err = SSTR("Unable to create resilvering-arena for '" << id << "'");
    qdb_critical(err);
    return false;
  }

  return true;
}

bool ShardDirectory::resilveringCopy(const ResilveringEventID &id, std::string_view filename,  std::string_view contents, std::string &err) {
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

std::string ShardDirectory::checkpoint(std::string_view path) {
  if(mkdir(std::string(path).c_str(), S_IRWXU) != 0) {
    return SSTR("Could not mkdir " << path << ": " << errno << " (" << strerror(errno) << ")");
  }

  std::string checkpointCurrent = pathJoin(path, "current");
  if(mkdir(checkpointCurrent.c_str(), S_IRWXU) != 0) {
    return SSTR("Could not mkdir " << checkpointCurrent << ": " << errno << " (" << strerror(errno) << ")");
  }

  std::string smCheckpoint = pathJoin(checkpointCurrent, "state-machine");
  rocksdb::Status st = getStateMachine()->checkpoint(smCheckpoint);
  if(!st.ok()) {
    std::string err = SSTR("Could not create state machine checkpoint in  " << smCheckpoint << ": " << st.ToString());
    qdb_critical(err);
    return err;
  }

  // TODO, switch to if(configuration.getMode() == Mode::raft)
  if(journalptr) {
    std::string journalCheckpoint = pathJoin(checkpointCurrent, "raft-journal");
    rocksdb::Status st = getRaftJournal()->checkpoint(journalCheckpoint);
    if(!st.ok()) {
      std::string err = SSTR("Could not create journal checkpoint in " << journalCheckpoint << ": " << st.ToString());
      qdb_critical(err);
      return err;
    }
  }

  std::string resilvHist = pathJoin(path, "RESILVERING-HISTORY");
  std::string err;
  if(!write_file(resilvHist, resilveringHistory.serialize(), err))  {
    qdb_critical(err);
    return err;
  }

  std::string shardIdentPath = pathJoin(path, "SHARD-ID");
  if(!write_file(shardIdentPath, shardID, err)) {
    qdb_critical(err);
    return err;
  }

  return {}; // success
}
