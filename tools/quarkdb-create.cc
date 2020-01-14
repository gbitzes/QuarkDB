// ----------------------------------------------------------------------
// File: quarkdb-create.cc
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
#include "raft/RaftJournal.hh"
#include "Common.hh"
#include "utils/FileUtils.hh"
#include "StateMachine.hh"
#include "../deps/CLI11.hpp"
#include "utils/FileUtils.hh"
#include <iostream>

struct PathValidator : public CLI::Validator {
  PathValidator() : Validator("PATH") {
    func_ = [](const std::string &path) {

      std::string err;
      if(quarkdb::directoryExists(path, err)) {
        return SSTR("'" << path << "' already exists.");
      }

      return std::string();
    };
  }
};

struct StealStateMachineValidator : public CLI::Validator {
  StealStateMachineValidator() : Validator("PATH") {
    func_ = [](const std::string &path) {

      std::string err;
      if(!quarkdb::directoryExists(path, err)) {
        return SSTR("Path '" << path << "' does not exist.");
      }

      return std::string();
    };
  }
};

struct NodeValidator : public CLI::Validator {
  NodeValidator() : Validator("NODES") {
    func_ = [](const std::string &str) {

      std::vector<quarkdb::RaftServer> servers;
      if(!quarkdb::parseServers(str, servers)) {
        return SSTR("Could not parse '" << str << "'. Expected format is a comma-separated list of servers: example1:1111,example2:2222");
      }

      return std::string();
    };
  }
};

int main(int argc, char** argv) {
  //----------------------------------------------------------------------------
  // Setup variables
  //----------------------------------------------------------------------------
  CLI::App app("Tool to initialize new QuarkDB nodes.");
  PathValidator pathValidator;
  NodeValidator nodeValidator;
  StealStateMachineValidator stealStateMachineValidator;

  std::string optPath;
  std::string optClusterID;
  std::string optNodes;
  std::string optStealStateMachine;

  //----------------------------------------------------------------------------
  // Setup options
  //----------------------------------------------------------------------------
  app.add_option("--path", optPath, "The location in which to create the new QuarkDB directory")
    ->required()
    ->check(pathValidator);

  auto clusterID = app.add_option("--clusterID", optClusterID, "Specify the cluster identifier for a new raft node - the ID needs to be globally unique for each separate cluster");

  app.add_option("--nodes", optNodes, "Specify the initial members of the new raft cluster.")
    ->needs(clusterID)
    ->check(nodeValidator);

  app.add_option("--steal-state-machine", optStealStateMachine, "Create the new node with the given pre-populated state-machine, which will be moved from the original folder, and not copied.")
    ->needs(clusterID)
    ->check(stealStateMachineValidator);

  //----------------------------------------------------------------------------
  // Setup footer with usage examples
  //----------------------------------------------------------------------------
  std::ostringstream footer;

  footer << std::endl << std::endl << "Recipes: " << std::endl;
  footer << " - To create a brand new standalone instance, run:" << std::endl;
  footer << "     $ quarkdb-create --path /directory/where/you/want/the/db" << std::endl << std::endl;

  footer << " - To create a brand new raft instance, run the following on _all_ participating nodes." << std::endl;
  footer << "   --clusterID and --nodes needs to be _identical_ across all invocations." << std::endl;
  footer << "     $ quarkdb-create --path /db/directory --clusterID unique-string-that-identifies-cluster --nodes host1:port1,host2:port2,host3:port3" << std::endl << std::endl;

  footer << " - To create a new cluster out of a bulkloaded instance:" << std::endl;
  footer << "     1. Shut down the bulkload node, if currently running." << std::endl;
  footer << "     2. Run $ quarkdb-create --path /db/directory --clusterID unique-string --nodes host1:port1,host2:port2,host3:port3 --steal-state-machine /path/to/bulkloaded/state/machine" << std::endl;
  footer << "     3. Using scp, stream over the network the entire contents of '/db/directory' to all of host1, host2, and host3." << std::endl;
  footer << "     4. No need to run quarkdb-create again - simply start up all nodes, they should form a quorum, and the contents will be the bulkloaded ones." << std::endl << std::endl;

  footer << " - To expand an existing cluster: " << std::endl;
  footer << "     1. Run $ quarkdb-create --path /db/directory --clusterID id-of-existing-cluster" << std::endl;
  footer << "        Note the omission of --nodes!" << std::endl;
  footer << "     2. Start up the node based on /db/directory. It will enter 'limbo mode', where it will sleep" << std::endl;
  footer << "        until it is contacted by the cluster." << std::endl;
  footer << "     3. In the current cluster leader, run redis command 'quarkdb-add-observer hostname_of_new_node:port" << std::endl;
  footer << "        This will cause the existing cluster to contact the newly created node, make it exit limbo mode, and bring it up-to-date." << std::endl;

  app.footer(footer.str());

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // One more quick check..
  //----------------------------------------------------------------------------
  if(!optStealStateMachine.empty() && !optClusterID.empty() && optNodes.empty()) {
    // Asking to create limbo-node with pre-populated state machine, disallow
    std::cerr << "--steal-state-machine: It makes no sense to initialize a node in limbo state with a pre-populated state machine." << std::endl;
    std::cerr << "Run with --help for more information." << std::endl;
    return 1;
  }

  //----------------------------------------------------------------------------
  // All good, let's roll.
  //----------------------------------------------------------------------------
  quarkdb::LogIndex journalStartingIndex = 0;
  std::unique_ptr<quarkdb::StateMachine> stolenStateMachine;

  // Are we stealing a state machine?
  if(!optStealStateMachine.empty()) {
    // Yes - extract its lastApplied. We'll need to sync-up the journal with this
    // number.

    std::string err;
    if(!quarkdb::directoryExists(optStealStateMachine, err)) {
      std::cerr << "Error accessing path given in --steal-state-machine: " << err << std::endl;
      exit(1);
    }

    // Use this unusual starting index for our journal to better protect against
    // the common mistake of:
    // - Start a raft cluster, where only a single node has the bulkloaded data,
    //   and the rest are clean.
    // - With a starting index of 1111, if a node without all data becomes leader,
    //   the cluster will blow up and the error will be detected.
    // - If the node with all data becomes leader, it'll just resilver the rest.
    journalStartingIndex = 1111;
    stolenStateMachine.reset(new quarkdb::StateMachine(optStealStateMachine));
  }

  quarkdb::Status st;
  std::unique_ptr<quarkdb::ShardDirectory> shardDirectory;
  if(!optClusterID.empty()) {
    std::vector<quarkdb::RaftServer> nodes;

    if(!optNodes.empty()) {
      quarkdb::parseServers(optNodes, nodes);
    }
    else {
      nodes.emplace_back(quarkdb::RaftServer::Null());
      qdb_info("--nodes were not specified. This new node will be 'in limbo' until it is contacted by an existing cluster, and cannot be used to start a new cluster from scratch. Run 'quarkdb-add-observer' on the leader of the existing cluster to add it.");
    }

    shardDirectory.reset(quarkdb::ShardDirectory::create(optPath, optClusterID, "default", nodes, journalStartingIndex, quarkdb::FsyncPolicy::kSyncImportantUpdates, std::move(stolenStateMachine), st));
  }
  else {
    shardDirectory.reset(quarkdb::ShardDirectory::create(optPath, "null", "default", std::move(stolenStateMachine), st));
  }

  if(!st.ok()) {
    std::cerr << "Error " << st.getErrc() << ": " << st.getMsg() << std::endl;
    return 1;
  }

  return 0;
}
