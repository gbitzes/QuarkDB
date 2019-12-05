// ----------------------------------------------------------------------
// File: quarkdb-server.cc
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

#include "utils/Macros.hh"
#include "utils/FileUtils.hh"
#include "utils/InFlightTracker.hh"
#include "Configuration.hh"
#include "QuarkDBNode.hh"
#include "netio/AsioPoller.hh"
#include "ShardDirectory.hh"
#include "StateMachine.hh"
#include "EventFD.hh"
#include "../deps/CLI11.hpp"
#include "utils/Uuid.hh"
#include "utils/DirectoryIterator.hh"

#include <dirent.h>

namespace quarkdb {

InFlightTracker inFlightTracker;
EventFD shutdownFD;

static void handle_sigint(int sig) {
  inFlightTracker.setAcceptingRequests(false);
  shutdownFD.notify();
}

void createTestCluster(const std::string &optConfigurationDir, int port) {
  quarkdb::mkpath_or_die(optConfigurationDir, 0755);

  std::string clusterID = generateUuid();

  std::vector<quarkdb::RaftServer> nodes;
  for(size_t i = 0; i < 3; i++) {
    nodes.emplace_back("localhost", port+i);
  }

  for(size_t i = 0; i < 3; i++) {

    std::string dataDir = SSTR(optConfigurationDir << "data-" << port);
    std::string configDir = SSTR(optConfigurationDir << "config-" << port);

    std::ostringstream ss;
    ss << "xrd.port " << port << std::endl;
    ss << "xrd.protocol redis:" << port << "libXrdQuarkDB.so" << std::endl;
    ss << "redis.mode raft" << std::endl;
    ss << "redis.database " << dataDir << std::endl;
    ss << "redis.myself localhost:" << port << std::endl;

    write_file_or_die(configDir, ss.str());
    std::unique_ptr<quarkdb::ShardDirectory> shardDirectory;

    quarkdb::Status st;
    shardDirectory.reset(quarkdb::ShardDirectory::create(dataDir, clusterID, "default", nodes, 0, {}, st));

    port++;
  }
}

int runCluster(const std::string &execPath, const std::string &optConfigurationDir, int port) {
  std::string err;
  if(!quarkdb::directoryExists(optConfigurationDir, err)) {
    createTestCluster(optConfigurationDir, port);
  }

  //----------------------------------------------------------------------------
  // Locate configuraiton files
  //----------------------------------------------------------------------------
  std::vector<std::string> configurationFiles;
  quarkdb::DirectoryIterator iter(optConfigurationDir);

  struct dirent *entry;
  while( (entry = iter.next()) ) {
    if(entry->d_type != DT_DIR && StringUtils::startsWith(entry->d_name, "config-")) {
      configurationFiles.emplace_back(SSTR(optConfigurationDir << entry->d_name));
    }
  }

  //----------------------------------------------------------------------------
  // Create new tmux session..
  //----------------------------------------------------------------------------
  std::string sessionId = generateUuid();
  system(SSTR("tmux -2 new-session -d -s \"" << sessionId << "\"").c_str());

  for(size_t i = 0; i < configurationFiles.size(); i++) {
    if(i != 0) {
      system("tmux split-window -v");
    }

    system(SSTR("tmux send-keys \"" << execPath << " --configuration " << configurationFiles[i] << " \" C-m").c_str());
  }

  system("tmux select-layout even-vertical");
  execlp("tmux", "tmux", "-2", "attach-session", "-d", (char*) NULL);
  return errno;
}

int runServer(const std::string &optConfiguration) {
  //----------------------------------------------------------------------------
  // Read configuration file, check validity..
  //----------------------------------------------------------------------------
  Configuration configuration;
  bool success = Configuration::fromFile(optConfiguration, configuration);
  if(!success) return 1;

  if(configuration.getMode() != Mode::raft) {
    qdb_throw("standalone mode not supported in quarkdb-server yet, sorry");
  }

  //----------------------------------------------------------------------------
  // Let's get this party started
  //----------------------------------------------------------------------------
  std::unique_ptr<QuarkDBNode> node(new QuarkDBNode(configuration, defaultTimeouts));
  std::unique_ptr<AsioPoller> poller(new AsioPoller(configuration.getMyself().port, 10, node.get()));

  signal(SIGINT, handle_sigint);
  signal(SIGTERM, handle_sigint);

  while(inFlightTracker.isAcceptingRequests()) {
    shutdownFD.wait();
  }

  //----------------------------------------------------------------------------
  // Time to shut down
  //----------------------------------------------------------------------------
  qdb_event("Received request to shut down. Waiting until all requests in flight (" << inFlightTracker.getInFlight() << ") have been processed..");

  poller.reset();
  node.reset();

  qdb_event("SHUTTING DOWN");
  return 0;
}

}

struct FileExistenceValidator : public CLI::Validator {
  FileExistenceValidator() : Validator("PATH") {
    func_ = [](const std::string &path) {

      std::string err;
      if(!quarkdb::fileExists(path, err)) {
        return SSTR("Path '" << path << "' does not exist, or is not a file.");
      }

      return std::string();
    };
  }
};

int main(int argc, char** argv) {
  //----------------------------------------------------------------------------
  // Setup variables
  //----------------------------------------------------------------------------
  CLI::App app("QuarkDB is a distributed datastore with a redis-like API. quarkdb-server is the main server executable.");
  FileExistenceValidator fileExistenceValidator;

  std::string optConfiguration;
  std::string optConfigurationDir;
  int clusterPort = 4444;

  //----------------------------------------------------------------------------
  // Setup options
  //----------------------------------------------------------------------------
  app.add_option("--configuration", optConfiguration, "Path to configuration file")
    ->check(fileExistenceValidator);

  app.add_option("--configuration-dir", optConfigurationDir, "Path to configuration directory to launch local test cluster - requires to have tmux installed.");
  app.add_option("--test-cluster-port", clusterPort, "The port to use when creating a local test cluster -- ignored if cluster configuration already existed.");

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);

    if(optConfiguration.empty() && optConfigurationDir.empty()) {
      std::cerr << "Either --configuration or --configuration-dir must be specified." << std::endl;
      return 1;
    }
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // Run cluster
  //----------------------------------------------------------------------------
  if(!optConfigurationDir.empty()) {
    if(optConfigurationDir[optConfigurationDir.size()-1] != '/') {
      optConfigurationDir += "/";
    }

    return quarkdb::runCluster(argv[0], optConfigurationDir, clusterPort);
  }

  //----------------------------------------------------------------------------
  // Run server.
  //----------------------------------------------------------------------------
  if(!optConfiguration.empty()) {
    return quarkdb::runServer(optConfiguration);
  }

  qdb_throw("should never reach here");
}

