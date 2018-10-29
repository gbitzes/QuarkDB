// ----------------------------------------------------------------------
// File: quarkdb-scrub.cc
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
#include <queue>

#include "raft/RaftJournal.hh"
#include "raft/RaftTalker.hh"
#include "raft/RaftUtils.hh"
#include "utils/ParseUtils.hh"

#include "Common.hh"
#include "OptionUtils.hh"

#define PIPELINE_LENGTH 5

namespace Opt {
enum  Type { UNKNOWN, HELP, NODES, START, END, VERBOSE, PIPELINE };
}

bool verify_options_sane(option::Parser &parse, std::vector<option::Option> &options) {
  if(parse.error()) {
    std::cout << "Parsing error" << std::endl;
    return false;
  }

  if(options[Opt::HELP]) {
    return false;
  }

  if(!options[Opt::NODES] || !options[Opt::START] || !options[Opt::END]) {
    std::cout << "--nodes, --start, and --end are required arguments." << std::endl;
    return false;
  }

  std::vector<quarkdb::RaftServer> servers;
  if(options[Opt::NODES] && !quarkdb::parseServers(options[Opt::NODES].arg, servers)) {
    std::cout << "Error parsing --nodes. Example of valid entry: server1:9000,server2:9000,server3:9000" << std::endl;
    return false;
  }

  if(servers.size() <= 1) {
    std::cout << "--nodes must specify at least two nodes." << std::endl;
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
    {Opt::UNKNOWN, 0, "", "", option::Arg::None, "quarkdb scrubbing tool. Contacts a number of nodes and verifies their journals are consistent with each other.\n"
                                                 "Scrubs the entries [start, end)."
                                                 "USAGE: quarkdb-scrub [options]\n\n" "Options:" },
    {Opt::HELP, 0, "", "help", option::Arg::None, " --help \tPrint usage and exit." },
    {Opt::NODES, 0, "", "nodes", Opt::nonempty, " --nodes \tspecify the initial configuration of the new cluster"},
    {Opt::START, 0, "", "start", Opt::numeric, " --start \tThe log index from which to start scrubbing"},
    {Opt::END, 0, "", "end", Opt::numeric, " --end \tThe log index at which to stop scrubbing."},
    {Opt::VERBOSE, 0, "", "verbose", option::Arg::None, " --verbose \tPrint all entries, even if there are no inconsistencies"},
    {Opt::PIPELINE, 0, "", "pipeline", Opt::numeric, " --pipeline \tPipeline this many fetch requests. High values could cause high server load. (default: 5)"},
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

using namespace quarkdb;

struct ReplyRound {
  LogIndex index;
  std::vector<std::future<redisReplyPtr>> replies;
};

// process replies for a single entry
void processReplies(ReplyRound &replyRound, bool verbose) {
  if(replyRound.index % 10000 == 0 || verbose) qdb_info("Processing journal entry #" << replyRound.index);

  std::vector<RaftEntry> entries;

  for(size_t i = 0; i < replyRound.replies.size(); i++) {
    RaftEntry entry;
    redisReplyPtr reply = replyRound.replies[i].get();

    if(!RaftParser::fetchResponse(reply.get(), entry)) qdb_critical("Reply for fetch entry #" << replyRound.index << " could not be parsed.");
    entries.emplace_back(std::move(entry));
  }

  // ascertain all entries are identical
  bool identical = true;
  for(size_t i = 1; i < entries.size(); i++) {
    if(entries[i] != entries[i-1]) {
      identical = false;
      break;
    }
  }

  if(!identical) {
    qdb_critical("journal inconsistency for entry #" << replyRound.index << ".");
  }
  else if(verbose) {
    qdb_info("journal entry #" << replyRound.index << " identical in all nodes.");
  }

  if(!identical || verbose) {
    for(size_t i = 0; i < entries.size(); i++) {
      qdb_info("#" << i << ": " << entries[i]);
    }
  }
}

void processQueue(std::queue<ReplyRound> &pendingReplies, size_t pipelineLength, bool verbose) {
  while(pendingReplies.size() >= (size_t) pipelineLength && !pendingReplies.empty()) {
    ReplyRound &replyRound = pendingReplies.front();
    processReplies(replyRound, verbose);
    pendingReplies.pop();
  }
}

int main(int argc, char** argv) {
  std::vector<option::Option> opts = parse_args(argc-1, argv+1);

  LogIndex start, end;
  quarkdb::ParseUtils::parseInt64(opts[Opt::START].arg, start);
  quarkdb::ParseUtils::parseInt64(opts[Opt::END].arg, end);

  std::vector<RaftServer> nodes;
  parseServers(opts[Opt::NODES].arg, nodes);

  std::vector<RaftTalker*> talkers;
  for(size_t i = 0; i < nodes.size(); i++) talkers.push_back(new RaftTalker(nodes[i]));

  int64_t pipelineLength = PIPELINE_LENGTH;
  if(opts[Opt::PIPELINE]) {
    quarkdb::ParseUtils::parseInt64(opts[Opt::PIPELINE].arg, pipelineLength);
  }

  std::queue<ReplyRound> pendingReplies;

  for(LogIndex index = start; index < end; index++) {
    ReplyRound replyRound;
    replyRound.index = index;

    for(size_t i = 0; i < nodes.size(); i++) {
      replyRound.replies.emplace_back(talkers[i]->fetch(index));
    }

    pendingReplies.emplace(std::move(replyRound));
    processQueue(pendingReplies, pipelineLength, opts[Opt::VERBOSE]);
  }

  processQueue(pendingReplies, 0, opts[Opt::VERBOSE]);
  return 0;
}
