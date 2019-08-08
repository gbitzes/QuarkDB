// ----------------------------------------------------------------------
// File: RecoveryRunner.hh
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

#ifndef QUARKDB_RECOVERY_RUNNER_HH
#define QUARKDB_RECOVERY_RUNNER_HH

#include "recovery/RecoveryRunner.hh"
#include "recovery/RecoveryDispatcher.hh"
#include "Poller.hh"

#include <vector>
#include <rocksdb/db.h>

namespace quarkdb {

class RecoveryRunner {
public:
  RecoveryRunner(const std::string &path, int port);
  static RedisEncodedResponse issueOneOffCommand(const std::string &path, RedisRequest &req);
  static void DumpTool(int argc, char** argv);

private:
  RecoveryEditor editor;
  RecoveryDispatcher dispatcher;
  Poller poller;
};

}

#endif
