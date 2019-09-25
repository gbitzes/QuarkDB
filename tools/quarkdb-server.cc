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

#include "utils/Macros.hh"
#include "utils/FileUtils.hh"
#include "utils/InFlightTracker.hh"
#include "Configuration.hh"
#include "QuarkDBNode.hh"
#include "netio/AsioPoller.hh"
#include "EventFD.hh"
#include "../deps/CLI11.hpp"

namespace quarkdb {

InFlightTracker inFlightTracker;
EventFD shutdownFD;

static void handle_sigint(int sig) {
  inFlightTracker.setAcceptingRequests(false);
  shutdownFD.notify();
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

struct ExistenceValidator : public CLI::Validator {
  ExistenceValidator() : Validator("PATH") {
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
  ExistenceValidator existenceValidator;

  std::string optConfiguration;

  //----------------------------------------------------------------------------
  // Setup options
  //----------------------------------------------------------------------------
  app.add_option("--configuration", optConfiguration, "Path to configuration file")
    ->required()
    ->check(existenceValidator);

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // Run server.
  //----------------------------------------------------------------------------
  return quarkdb::runServer(optConfiguration);
}

