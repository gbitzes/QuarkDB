//------------------------------------------------------------------------------
// File: test-config.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#include <climits>
#include "test-config.hh"
#include "utils/ParseUtils.hh"
#include <unistd.h>

using namespace quarkdb;
TestConfig quarkdb::testconfig;

// parse environment variables to give the possibility to override defaults
TestConfig::TestConfig() {
  int i = 1;
  char *s = *environ;

  for (; s; i++) {
    std::string var(s);
    if(startswith(var, "QDB_")) {
      std::vector<std::string> chunks = split(var, "=");
      if(chunks.size() != 2) {
        std::cerr << "Could not parse environment variable: " << var << std::endl;
        exit(EXIT_FAILURE);
      }

      parseSingle(chunks[0], chunks[1]);
    }
    s = *(environ+i);
  }
}

void TestConfig::parseSingle(const std::string &key, const std::string &value) {
  std::cerr << "Applying runtime configuration option: " << key << " => " << value << std::endl;

  if(key == "QDB_TEST_TIMEOUT") {
    if(value == "aggressive") {
      return raftTimeouts.setRuntime(aggressiveTimeouts);
    }

    if(value == "tight") {
      return raftTimeouts.setRuntime(tightTimeouts);
    }

    if(value == "default") {
      return raftTimeouts.setRuntime(defaultTimeouts);
    }

    goto parse_error;
  }

  if(key == "QDB_TESTS_DATABASE_REUSE") {
    if(value == "yes") {
      return databaseReuse.setRuntime(true);
    }

    if(value == "no") {
      return databaseReuse.setRuntime(false);
    }

    goto parse_error;
  }

  if(key == "QDB_BENCH_THREADS") {
    std::vector<int64_t> threads;
    if(!ParseUtils::parseIntegerList(value, ",", threads)) {
      goto parse_error;
    }

    return benchmarkThreads.setRuntime(threads);
  }

  if(key == "QDB_BENCH_EVENTS") {
    std::vector<int64_t> events;
    if(!ParseUtils::parseIntegerList(value, ",", events)) {
      goto parse_error;
    }

    return benchmarkEvents.setRuntime(events);
  }

  qdb_throw("Unknown configuration option: " << key << " => " << value);

parse_error:
  qdb_throw("Cannot parse configuration value for key " << key << ": '" << value << "'");
}
