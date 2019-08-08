// ----------------------------------------------------------------------
// File: InternalFilter.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "redis/InternalFilter.hh"
#include "RedisRequest.hh"
using namespace quarkdb;

void InternalFilter::process(RedisRequest &req) {
  switch(req.getCommand()) {
    case RedisCommand::TIMESTAMPED_LEASE_RELEASE:
    case RedisCommand::TIMESTAMPED_LEASE_ACQUIRE:
    case RedisCommand::TIMESTAMPED_LEASE_GET: {
      // Bad client, bad. No cookie for you.
      req.invalidate();
    }
    default: {
      // All is good, do nothing.
      return;
    }
  }
}
