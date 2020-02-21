// ----------------------------------------------------------------------
// File: quarkdb-validate-checkpoint.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "../deps/CLI11.hpp"
#include "utils/FileUtils.hh"
#include "ShardDirectory.hh"
#include "StateMachine.hh"
#include "raft/RaftJournal.hh"

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

struct PathValidator : public CLI::Validator {
  PathValidator() : Validator("PATH") {
    func_ = [](const std::string &path) {

      std::string err;
      if(!quarkdb::directoryExists(path, err)) {
        return SSTR("'" << path << "' does not exist.");
      }

      return std::string();
    };
  }
};

int main(int argc, char** argv) {
  //----------------------------------------------------------------------------
  // Setup variables
  //----------------------------------------------------------------------------
  CLI::App app("Tool to validate QuarkDB checkpoints (backups)");

  PathValidator pathValidator;
  std::string optPath;
  bool acceptStandalone = false;
  bool eos = false;

  //----------------------------------------------------------------------------
  // Setup options
  //----------------------------------------------------------------------------
  app.add_option("--path", optPath, "The path to the QuarkDB checkpoint")
    ->required()
    ->check(pathValidator);

  app.add_flag("--accept-standalone", acceptStandalone, "No need to ensure that the raft journal is present -- use this flag for standalone instances");
  app.add_flag("--eos", eos, "This QuarkDB instance contains the EOS namespace; additionally check eos-files-md and eos-containers-md");

  //----------------------------------------------------------------------------
  // Parse
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // Can we set-up a ShardDirectory?
  //----------------------------------------------------------------------------
  qdb_info("Attempting to open ShardDirectory...");
  quarkdb::ShardDirectory shardDirectory(optPath);
  qdb_info("--- OK!");

  //----------------------------------------------------------------------------
  // Can we open the StateMachine?
  //----------------------------------------------------------------------------
  qdb_info("Attempting to open StateMachine...");
  quarkdb::StateMachine *stateMachine = shardDirectory.getStateMachine();
  qdb_info("--- OK! LAST-APPLIED: " << stateMachine->getLastApplied());

  //----------------------------------------------------------------------------
  // Check eos namespace parameters?
  //----------------------------------------------------------------------------
  if(eos) {
    size_t len = 0;

    rocksdb::Status st = stateMachine->lhlen("eos-files-md", len);
    if(!st.ok()) {
      qdb_error("Status not ok when retrieving eos-files-md: " << st.ToString());
      return 1;
    }

    if(len == 0) {
      qdb_error("eos-files-md length is zero!");
      return 1;
    }

    qdb_info("eos-files-md length: " << len);

    st = stateMachine->lhlen("eos-containers-md", len);
    if(!st.ok()) {
      qdb_error("Status not ok when retrieving eos-containers-md: " << st.ToString());
      return 1;
    }

    if(len == 0) {
      qdb_error("eos-containers-md length is zero!");
      return 1;
    }

    qdb_info("eos-containers-md length: " << len);
  }

  //----------------------------------------------------------------------------
  // Does the raft journal directory even exist?
  //----------------------------------------------------------------------------
  std::string err;
  if(!shardDirectory.hasRaftJournal(err)) {

    if(acceptStandalone) {
      qdb_info("raft-journal directory not found, likely a standalone instance");
      return 0;
    }

    qdb_error("raft-journal not found!");
    return 1;
  }

  //----------------------------------------------------------------------------
  // Yes, let's open it
  //----------------------------------------------------------------------------
  qdb_info("Attempting to open RaftJournal...");
  quarkdb::RaftJournal *raftJournal = shardDirectory.getRaftJournal();
  qdb_info("--- OK! LOG-SIZE: " << raftJournal->getLogSize() << ", COMMIT-INDEX: " << raftJournal->getCommitIndex() << ", LOG-START: " << raftJournal->getLogStart());

  //----------------------------------------------------------------------------
  // Ensure LAST-APPLIED makes sense
  //----------------------------------------------------------------------------
  if(stateMachine->getLastApplied() > raftJournal->getCommitIndex()) {
    qdb_error("LAST-APPLIED does not make sense given the current COMMIT-INDEX!");
    return 1;
  }

  if(stateMachine->getLastApplied() < raftJournal->getLogStart()) {
    qdb_error("LAST-APPLIED does not make sense given the current LOG-START!");
    return 1;
  }

  return 0;
}
