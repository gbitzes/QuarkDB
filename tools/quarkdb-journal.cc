// ----------------------------------------------------------------------
// File: quarkdb-journal.cc
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
#include "raft/RaftJournal.hh"
#include "Common.hh"
#include "OptionUtils.hh"

namespace Opt {
enum  Type { UNKNOWN, HELP, PATH, CREATE, CLUSTERID, NODES };
}

bool verify_options_sane(option::Parser &parse, std::vector<option::Option> &options) {
  if(parse.error()) {
    std::cout << "Parsing error" << std::endl;
    return false;
  }

  if(options[Opt::HELP]) {
    return false;
  }

  if(options[Opt::CREATE] && (!options[Opt::PATH] || !options[Opt::CLUSTERID] || !options[Opt::NODES])) {
    std::cout << "--path, --clusterID, and --nodes are required when using --create." << std::endl;
    return false;
  }

  if(!options[Opt::CREATE]) {
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
    {Opt::UNKNOWN, 0, "", "", option::Arg::None, "quarkdb journal inspector. It can only create new ones right now.\n"
                                                 "USAGE: quarkdb-journal [options]\n\n" "Options:" },
    {Opt::HELP, 0, "", "help", option::Arg::None, " --help \tPrint usage and exit." },
    {Opt::PATH, 0, "", "path", Opt::nonempty, " --path \tthe directory where the journal lives in."},
    {Opt::CREATE, 0, "", "create", option::Arg::None, " --create \tcreate a new raft journal, used with --clusterID and --nodes"},
    {Opt::CLUSTERID, 0, "", "clusterID", Opt::nonempty, " --clusterID \tspecify the clusterID of the new journal"},
    {Opt::NODES, 0, "", "nodes", Opt::nonempty, " --nodes \tspecify the initial configuration of the new cluster"},

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

std::string retrieve(const std::vector<option::Option> &options,  const Opt::Type key) {
  if(!options[key]) return "";
  return options[key].arg;
}

int createJournal(const std::vector<option::Option> &opts) {
  std::vector<quarkdb::RaftServer> nodes;
  quarkdb::parseServers(opts[Opt::NODES].arg, nodes);

  quarkdb::RaftJournal::ObliterateAndReinitializeJournal(opts[Opt::PATH].arg, opts[Opt::CLUSTERID].arg, nodes, 0);
  return 0;
}

int main(int argc, char** argv) {
  std::vector<option::Option> opts = parse_args(argc-1, argv+1);
  if(opts[Opt::CREATE]) {
    createJournal(opts);
  }

  return 0;
}
