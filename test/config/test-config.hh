//------------------------------------------------------------------------------
// File: test-config.hh
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

#ifndef __QUARKDB_TEST_CONFIG_H__
#define __QUARKDB_TEST_CONFIG_H__

#include "raft/RaftTimeouts.hh"
#include "Utils.hh"

namespace quarkdb {

class TestConfig;

// A configuration option which can be given a global default, a local default
// through setStatic, but also overriden during runtime through environment
// variables.
template<typename T>
class ConfigurationOption {
public:
  template<typename... Args>
  ConfigurationOption(Args&&... args) : value{std::forward<Args>(args)...} {}

  T* operator->() {
    return &value;
  }

  operator T() {
    return value;
  }

  T get() {
    return value;
  }

  void setStatic(const T& val) {
    if(overridenAtRuntime) return;
    value = val;
  }

private:
  T value;
  bool overridenAtRuntime = false;

  void setRuntime(const T& val) {
    overridenAtRuntime = true;
    value = val;
  }

  friend class TestConfig;
};

struct TestConfig {
  // parse environment variables to give the possibility to override defaults
  TestConfig();
  void parseSingle(const std::string &key, const std::string &value);

  ConfigurationOption<RaftTimeouts> raftTimeouts {aggressiveTimeouts};
  ConfigurationOption<bool> databaseReuse {true};

  ConfigurationOption<std::vector<int64_t>> benchmarkThreads {1, 2, 4, 8};
  ConfigurationOption<std::vector<int64_t>> benchmarkEvents {1000000};

};

extern TestConfig testconfig;
}

#endif
