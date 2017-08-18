// ----------------------------------------------------------------------
// File: RequestCounter.hh
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

#ifndef __QUARKDB_REQUEST_COUNTER_H__
#define __QUARKDB_REQUEST_COUNTER_H__

#include <atomic>
#include "../Utils.hh"
#include "AssistedThread.hh"

namespace quarkdb {

class RedisRequest; class WriteBatch;

//------------------------------------------------------------------------------
// Count what types of requests we've been servicing, and reports statistics
// every few seconds.
//------------------------------------------------------------------------------

class RequestCounter {
public:
  RequestCounter(std::chrono::seconds interval);

  void account(const RedisRequest &req);
  void account(const WriteBatch &batch);
  void mainThread(ThreadAssistant &assistant);
private:
  std::atomic<int64_t> reads {0};
  std::atomic<int64_t> writes {0};
  std::atomic<int64_t> batches {0};
  bool paused = true;

  std::chrono::seconds interval;
  AssistedThread thread;

};

}

#endif
