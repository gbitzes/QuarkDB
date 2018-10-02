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
#include "OptionUtils.hh"
#include "utils/ParseUtils.hh"
#include "Utils.hh"
#include "qclient/QClient.hh"

namespace Opt {
enum  Type { UNKNOWN, HELP, PATH, PORT, COMMAND };
}

bool verify_options_sane(option::Parser &parse, std::vector<option::Option> &options) {
  if(parse.error()) {
    std::cout << "Parsing error" << std::endl;
    return false;
  }

  if(options[Opt::HELP]) {
    return false;
  }

  if(!options[Opt::PATH]) {
    std::cout << "--path is required." << std::endl;
    return false;
  }

  if(!options[Opt::PORT] && !options[Opt::COMMAND]) {
    std::cout << "either --port or --command is required." << std::endl;
    return false;
  }

  for(option::Option* opt = options[Opt::UNKNOWN]; opt; opt = opt->next()) {
    std::cout << "Unknown option: " << std::string(opt->name,opt->namelen) << "\n";
    return false;
  }

  for(int i = 0; i < parse.nonOptionsCount(); ++i) {
    std::cout << "Non-option #" << i << ": " << parse.nonOption(i) << "\n";
    return false;
  }

  return true;
}


std::vector<option::Option> parse_args(int argc, char** argv) {
  const option::Descriptor usage[] = {
    {Opt::UNKNOWN, 0, "", "", option::Arg::None, "Low-level access to QuarkDB databases.\n"
                                                 "USAGE: quarkdb-recovery [options]\n\n" "Options:" },
    {Opt::HELP, 0, "", "help", option::Arg::None, " --help \tPrint usage and exit." },
    {Opt::PATH, 0, "", "path", Opt::nonempty, " --path \tthe directory where the state-machine or journal lives in."},
    {Opt::PORT, 0, "", "port", Opt::numeric, " --port \tthe port to listen to."},
    {Opt::COMMAND, 0, "", "command", Opt::nonempty, " --command \tone-off command to run"},

    {0,0,0,0,0,0}
  };

  option::Stats stats(usage, argc, argv);
  std::vector<option::Option> options(stats.options_max);
  std::vector<option::Option> buffer(stats.buffer_max);
  option::Parser parse(usage, argc, argv, &options[0], &buffer[0]);

  if(!verify_options_sane(parse, options)) {
    option::printUsage(std::cout, usage);
    exit(1);
  }
  return options;
}

void run(std::string path, int port, quarkdb::ThreadAssistant &assistant) {
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

int main(int argc, char** argv) {
  std::vector<option::Option> opts = parse_args(argc-1, argv+1);
  std::string path = opts[Opt::PATH].arg;

  if(opts[Opt::PORT]) {
    int64_t port = 0;
    quarkdb::ParseUtils::parseInt64(opts[Opt::PORT].arg, port);

    th.reset(run, path, port);

    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);

    th.blockUntilThreadJoins();
  }
  else {
    oneOffCommand(path, opts[Opt::COMMAND].arg);
  }

  return 0;
}
