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

#ifndef __QUARKDB_RECOVERY_RUNNER_H__
#define __QUARKDB_RECOVERY_RUNNER_H__

#include "RecoveryRunner.hh"
#include "RecoveryDispatcher.hh"
#include "../Poller.hh"

#include <vector>
#include <rocksdb/db.h>

namespace quarkdb {

class RecoveryRunner {
public:
  RecoveryRunner(const std::string &path, int port);
  static RedisEncodedResponse issueOneOffCommand(const std::string &path, RedisRequest &req);

private:
  RecoveryEditor editor;
  RecoveryDispatcher dispatcher;
  Poller poller;
};

}

#endif
