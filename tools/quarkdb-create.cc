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

#include <iostream>
#include "ShardDirectory.hh"
#include "raft/RaftJournal.hh"
#include "Common.hh"
#include "OptionUtils.hh"

namespace Opt {
enum  Type { UNKNOWN, HELP, PATH, MODE, CLUSTERID, NODES};
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

  if( (options[Opt::CLUSTERID] && !options[Opt::NODES]) || (!options[Opt::CLUSTERID] && options[Opt::NODES]) ) {
    std::cout << "Both --clusterID and --nodes need to be provided at the same time." << std::endl;
    return false;
  }

  std::vector<quarkdb::RaftServer> servers;
  if(options[Opt::NODES] && !quarkdb::parseServers(options[Opt::NODES].arg, servers)) {
    std::cout << "Error parsing --nodes. Example of valid entry: server1:9000,server2:9000,server3:9000" << std::endl;
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
    {Opt::UNKNOWN, 0, "", "", option::Arg::None, "Tool to initialize new quarkdb nodes.\n"
                                                 "USAGE: quarkdb-journal [options]\n\n" "Options:" },
    {Opt::HELP, 0, "", "help", option::Arg::None, " --help \tPrint usage and exit." },
    {Opt::PATH, 0, "", "path", Opt::nonempty, " --path \tthe directory where the journal lives in."},
    {Opt::CLUSTERID, 0, "", "clusterID", Opt::nonempty, " --clusterID \tspecify the identifier for the new cluster - should be globally unique."},
    {Opt::NODES, 0, "", "nodes", Opt::nonempty, " --nodes \tspecify the initial members of the new cluster. If not specified, quarkdb will start in standalone mode."},

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

int main(int argc, char** argv) {
  std::vector<option::Option> opts = parse_args(argc-1, argv+1);

  quarkdb::ShardDirectory *shardDirectory;
  if(opts[Opt::NODES]) {
    std::vector<quarkdb::RaftServer> nodes;
    quarkdb::parseServers(opts[Opt::NODES].arg, nodes);
    shardDirectory = quarkdb::ShardDirectory::create(opts[Opt::PATH].arg, opts[Opt::CLUSTERID].arg, "default", nodes);
  }
  else {
    shardDirectory = quarkdb::ShardDirectory::create(opts[Opt::PATH].arg, "null", "default");
  }

  delete shardDirectory;

  return 0;
}
