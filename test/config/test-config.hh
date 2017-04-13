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

struct TestConfig {
  // parse environment variables to give the possibility to override defaults
  TestConfig();
  void parseSingle(const std::string &key, const std::string &value);


  RaftTimeouts raftTimeouts {aggressiveTimeouts};
};

extern TestConfig testconfig;
}

#endif
