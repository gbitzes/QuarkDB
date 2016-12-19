// ----------------------------------------------------------------------
// File: quarkdb-chaos.cc
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
#include "Common.hh"
#include "OptionUtils.hh"
#include "Utils.hh"
#include <qclient/qclient.hh>
#include <thread>
#include <random>
#include "TestUtils.hh"

using namespace qclient;

namespace Opt {
enum  Type { UNKNOWN, HELP, NODES, PREFIX, RANGE, ITERATIONS, PIPELINE, THREADS};
}

bool verify_options_sane(option::Parser &parse, std::vector<option::Option> &options) {
  if(parse.error()) {
    std::cout << "Parsing error" << std::endl;
    return false;
  }

  if(options[Opt::HELP]) {
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

static std::random_device rd;
static std::mt19937 mt(rd());

bool assert_ok(const redisReplyPtr &ptr) {
  if(!ptr) {
    qdb_warn("Received null reply");
    return false;
  }

  if(ptr->type != REDIS_REPLY_STATUS) {
    qdb_critical("Unexpected reply type: " << ptr->type);
    return false;
  }

  if(std::string(ptr->str, ptr->len) != "OK") {
    qdb_critical("Wrong status message: " << std::string(ptr->str, ptr->len));
    return false;
  }

  return true;
}

int64_t extract_timestamp(const redisReplyPtr &ptr, const std::string &key, int64_t earliestTimestamp) {
  if(!ptr) {
    qdb_warn("Received null reply");
    return -1;
  }

  if(ptr->type == REDIS_REPLY_ERROR) {
    qdb_critical("Unexpected error: " << std::string(ptr->str, ptr->len));
    return -1;
  }

  if(ptr->type == REDIS_REPLY_NIL) {
    return -1;
  }

  if(ptr->type != REDIS_REPLY_STRING) {
    qdb_critical("Unexpected reply type: " << ptr->type);
    return -1;
  }

  std::string str(std::string(ptr->str, ptr->len));
  int64_t nanoseconds = stoll(str);

  if(nanoseconds < earliestTimestamp) {
    qdb_critical("Received " << nanoseconds << " while earliest timestamp is " << earliestTimestamp << " for key " << key);
  }

  return nanoseconds;
}

Cache cache;
std::mutex mtx;
std::atomic<int64_t> counter {0};

void testReadWriteString(QClient &tunnel, int64_t range, const std::string &prefix, int iterations, int pipeline, const std::vector<quarkdb::RaftServer> &nodes, int nodeId) {
  std::uniform_int_distribution<int64_t> dist(0, range);
  std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

  for(int iteration = 0; iteration < iterations; iteration++) {
    std::vector<std::future<redisReplyPtr>> futures;
    std::vector<int64_t> lastUpdated;
    std::vector<std::string> keys;

    // send off
    for(int i = 0; i < pipeline; i++) {
      std::string key = SSTR(prefix << "-" << dist(mt));
      keys.emplace_back(key);

      lock.lock();
      lastUpdated.emplace_back(cache.get(key));
      futures.emplace_back(tunnel.exec("GET", key));

      int64_t now = counter++;
      lastUpdated.emplace_back(now);
      futures.emplace_back(tunnel.exec("SET", key, SSTR(now)));

      lastUpdated.emplace_back(cache.get(key));
      futures.emplace_back(tunnel.exec("GET", key));
      lock.unlock();
    }

    // check results
    for(int i = 0; i < pipeline; i++) {
      redisReplyPtr get1 = futures[i*3].get();
      int64_t ts1 = -1;
      if(cache.get(keys[i]) > 0) {
        ts1 = extract_timestamp(get1, keys[i], lastUpdated[i*3]);
      }

      redisReplyPtr put = futures[i*3+1].get();
      if(assert_ok(put)) {
        cache.put(keys[i], lastUpdated[i*3+1]);
      }

      redisReplyPtr get2 = futures[i*3+2].get();
      int64_t ts2 = extract_timestamp(get2, keys[i], lastUpdated[i*3+2]);

      if(ts1 >= ts2) {
        std::cout << "key: " << keys[i] << ", ts1 " << ts1 << " ts2 " << ts2 << " result from put: " << lastUpdated[i*3+1] << std::endl;
      }
    }
  }
}

std::vector<option::Option> parse_args(int argc, char** argv) {
  const option::Descriptor usage[] = {
    {Opt::UNKNOWN, 0, "", "", option::Arg::None, "quarkdb chaos testing tool. DO NOT RUN AGAINST PRODUCTION INSTANCES.\n"
                                                 "USAGE: quarkdb-chaos [options]\n\n" "Options:" },
    {Opt::HELP, 0, "", "help", option::Arg::None, " --help \tPrint usage and exit." },
    {Opt::NODES, 0, "", "nodes", Opt::nonempty, " --nodes \tspecify the list of nodes in the cluster"},
    {Opt::PREFIX, 0, "", "prefix", Opt::nonempty, " --prefix \tall keys will be prepended with this prefix (default: 'chaos-')"},
    {Opt::RANGE, 0, "", "range", Opt::numeric, " --prefix \tthe maximum integer to append to keys (default: 100)"},
    {Opt::ITERATIONS, 0, "", "iterations", Opt::numeric, " --iterations \tthe number of read-write-read iterations to perform per thread (default: 10000)"},
    {Opt::PIPELINE, 0, "", "pipeline", Opt::numeric, " --pipeline \tthe number of requests to pipeline per iteration (default: 100)"},
    {Opt::THREADS, 0, "", "threads", Opt::numeric, " --threads \tthe number of parallel threads to use (default: 1)"},

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


int main(int argc, char **argv) {
  std::vector<option::Option> opts = parse_args(argc-1, argv+1);

  std::string prefix = "chaos-";
  if(opts[Opt::PREFIX]) prefix = opts[Opt::PREFIX].arg;

  int64_t range = 100;
  if(opts[Opt::RANGE]) range = atoi(opts[Opt::RANGE].arg);

  std::vector<quarkdb::RaftServer> nodes;
  parseServers(opts[Opt::NODES].arg, nodes);

  int64_t iterations = 10000;
  if(opts[Opt::ITERATIONS]) iterations = atoi(opts[Opt::ITERATIONS].arg);

  int64_t pipeline = 100;
  if(opts[Opt::PIPELINE]) pipeline = atoi(opts[Opt::PIPELINE].arg);

  int64_t nthreads = 1;
  if(opts[Opt::THREADS]) nthreads = atoi(opts[Opt::THREADS].arg);

  QClient tunnel(nodes[0].hostname, nodes[0].port, true);

  std::vector<std::thread> threads;
  for(int i = 0; i < nthreads; i++) {
    std::cout << "Starting thread #" << i << std::endl;
    threads.emplace_back(testReadWriteString, std::ref(tunnel), range, prefix, iterations, pipeline, nodes, 0);
  }

  for(int i = 0; i < nthreads; i++) {
    threads[i].join();
  }

  return 0;
}
