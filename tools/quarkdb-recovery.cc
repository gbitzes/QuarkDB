// ----------------------------------------------------------------------
// File: quarkdb-recovery.cc
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

#include <iostream>
#include "ShardDirectory.hh"
#include "raft/RaftJournal.hh"
#include "utils/AssistedThread.hh"
#include "recovery/RecoveryRunner.hh"
#include "Common.hh"
#include "utils/ParseUtils.hh"
#include "Utils.hh"
#include "qclient/QClient.hh"
#include "utils/FileUtils.hh"
#include "../deps/CLI11.hpp"

void run(const std::string &path, int port, quarkdb::ThreadAssistant &assistant) {
  quarkdb::RecoveryRunner runner(path, port);

  while(!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(1));
  }
}

quarkdb::AssistedThread th;

static void handle_sigint(int sig) {
  th.stop();
}

void oneOffCommand(const std::string &path, const std::string &cmd) {
  std::vector<std::string> command = quarkdb::split(cmd, " ");

  quarkdb::RedisRequest req;
  for(auto it = command.begin(); it != command.end(); it++) {
    req.push_back(*it);
  }

  quarkdb::RedisEncodedResponse response = quarkdb::RecoveryRunner::issueOneOffCommand(path, req);

  qclient::ResponseBuilder builder;
  builder.feed(response.val);

  qclient::redisReplyPtr reply;
  qdb_assert(builder.pull(reply) == qclient::ResponseBuilder::Status::kOk);
  std::cout << qclient::describeRedisReply(reply) << std::endl;
}

struct ExistenceValidator : public CLI::Validator {
  ExistenceValidator() : Validator("PATH") {
    func_ = [](const std::string &path) {

      std::string err;
      if(!quarkdb::directoryExists(path, err)) {
        return SSTR("Path '" << path << "' does not exist.");
      }

      return std::string();
    };
  }
};


int main(int argc, char** argv) {
  //----------------------------------------------------------------------------
  // Setup variables
  //----------------------------------------------------------------------------
  CLI::App app("Tool for low-level inspection of QuarkDB databases.");
  ExistenceValidator existenceValidator;

  std::string optPath;
  int optPort;
  std::string optOneOffCommand;

  //----------------------------------------------------------------------------
  // Setup options
  //----------------------------------------------------------------------------
  app.add_option("--path", optPath, "The path to the rocksdb directory to inspect")
    ->required()
    ->check(existenceValidator);

  auto actionGroup = app.add_option_group("Action", "Specify what action to take with the specified database directory");
  actionGroup->add_option("--port", optPort, "Launch a server listening for redis commands at this port, supporting special debugging and recovery commands.");
  actionGroup->add_option("--command", optOneOffCommand, "Instead of launching a server, issue a quick one-off recovery command.");
  actionGroup->require_option(1, 1);

  //----------------------------------------------------------------------------
  // Parse..
  //----------------------------------------------------------------------------
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }

  //----------------------------------------------------------------------------
  // All good, let's roll.
  //----------------------------------------------------------------------------
  if(!optOneOffCommand.empty()) {
    oneOffCommand(optPath, optOneOffCommand);
    return 0;
  }
  else {
    th.reset(run, optPath, optPort);

    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);

    th.blockUntilThreadJoins();
  }

  return 0;
}
