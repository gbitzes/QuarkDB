// ----------------------------------------------------------------------
// File: LeaseFilter.cc
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

#include "../RedisRequest.hh"
#include "LeaseFilter.hh"
#include "../Commands.hh"
#include "../utils/Macros.hh"
#include "../utils/IntToBinaryString.hh"
#include "../Formatter.hh"

using namespace quarkdb;

void LeaseFilter::transform(RedisRequest &req, ClockValue timestamp) {
  qdb_assert(req.getCommand() == RedisCommand::LEASE_GET || req.getCommand() == RedisCommand::LEASE_ACQUIRE);

  if(req.getCommand() == RedisCommand::LEASE_GET) {
    req[0] = "TIMESTAMPED_LEASE_GET";
    req.emplace_back(unsignedIntToBinaryString(timestamp));
    req.parseCommand();
    return;
  }
  else if(req.getCommand() == RedisCommand::LEASE_ACQUIRE) {
    req[0] = "TIMESTAMPED_LEASE_ACQUIRE";
    req.emplace_back(unsignedIntToBinaryString(timestamp));
    req.parseCommand();
    return;
  }

  qdb_throw("should never reach here");
}
