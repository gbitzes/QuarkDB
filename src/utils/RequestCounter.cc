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
#include "../redis/Transaction.hh"
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

void RequestCounter::account(const Transaction &transaction) {
  batches++;

  for(size_t i = 0; i < transaction.size(); i++) {
    account(transaction[i]);
  }
}

std::string RequestCounter::toRate(int64_t val) {
  return SSTR("(" << val / interval.count() << " Hz)");
}

void RequestCounter::setReportingStatus(bool val) {
  activated = val;
}

void RequestCounter::mainThread(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {

    int64_t localReads = reads.exchange(0);
    int64_t localWrites = writes.exchange(0);
    int64_t localBatches = batches.exchange(0);

    if(localReads != 0 || localWrites != 0) {
      paused = false;
      if(activated) {
        qdb_info("Over the last " << interval.count() << " seconds, I serviced " << localReads << " reads " << toRate(localReads) <<  ", and " << localWrites << " writes " << toRate(localWrites) << ". Processed " << localBatches << " batches.");
      }
    }
    else if(!paused) {
      paused = true;
      if(activated) {
        qdb_info("No reads or writes during the last " << interval.count() << " seconds - will report again once load re-appears.");
      }
    }

    assistant.wait_for(interval);
  }
}
