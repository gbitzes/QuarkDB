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

#include "RedisRequest.hh"
#include "redis/LeaseFilter.hh"
#include "Commands.hh"
#include "redis/Transaction.hh"
#include "utils/Macros.hh"
#include "utils/IntToBinaryString.hh"
#include "Formatter.hh"

using namespace quarkdb;


void LeaseFilter::transform(Transaction &tx, ClockValue timestamp) {
  for(size_t i = 0; i < tx.size(); i++) {
    if(tx[i].getCommand() == RedisCommand::LEASE_GET || tx[i].getCommand() == RedisCommand::LEASE_ACQUIRE || tx[i].getCommand() == RedisCommand::LEASE_RELEASE) {
      LeaseFilter::transform(tx[i], timestamp);
    }
  }
}

void LeaseFilter::transform(RedisRequest &req, ClockValue timestamp) {
  qdb_assert(req.getCommand() == RedisCommand::LEASE_GET || req.getCommand() == RedisCommand::LEASE_ACQUIRE || req.getCommand() == RedisCommand::LEASE_RELEASE);

  if(req.getCommand() == RedisCommand::LEASE_GET) {
    req.getPinnedBuffer(0) = PinnedBuffer("TIMESTAMPED_LEASE_GET");
    req.emplace_back(unsignedIntToBinaryString(timestamp));
    req.parseCommand();
    return;
  }
  else if(req.getCommand() == RedisCommand::LEASE_ACQUIRE) {
    req.getPinnedBuffer(0) = PinnedBuffer("TIMESTAMPED_LEASE_ACQUIRE");
    req.emplace_back(unsignedIntToBinaryString(timestamp));
    req.parseCommand();
    return;
  }
  else if(req.getCommand() == RedisCommand::LEASE_RELEASE) {
    req.getPinnedBuffer(0) = PinnedBuffer("TIMESTAMPED_LEASE_RELEASE");
    req.emplace_back(unsignedIntToBinaryString(timestamp));
    req.parseCommand();
    return;
  }

  qdb_throw("should never reach here");
}
