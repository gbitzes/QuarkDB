// ----------------------------------------------------------------------
// File: RequestCounter.cc
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

#include "../Utils.hh"
#include "RequestCounter.hh"
#include "../Commands.hh"
using namespace quarkdb;

RequestCounter::RequestCounter(std::chrono::seconds intv)
  : interval(intv), thread(&RequestCounter::mainThread, this) { }

void RequestCounter::account(const RedisRequest &req) {
  if(req.getCommandType() == CommandType::READ) {
    reads++;
  }
  else if(req.getCommandType() == CommandType::WRITE) {
    writes++;
  }
}

void RequestCounter::mainThread(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {

    int64_t localReads = reads.exchange(0);
    int64_t localWrites = writes.exchange(0);

    if(localReads != 0 || localWrites != 0) {
      qdb_info("Over the last " << interval.count() << " seconds, I serviced reads at a rate of " << localReads / interval.count() << " Hz, and writes at " << localWrites / interval.count() << " Hz");
    }

    assistant.wait_for(interval);
  }
}
