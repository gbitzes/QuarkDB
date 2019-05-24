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
#include "AssistedThread.hh"
#include "Statistics.hh"

namespace quarkdb {

class Transaction;
class RedisRequest;
class Statistics;

//------------------------------------------------------------------------------
// Count what types of requests we've been servicing, and reports statistics
// every few seconds.
//------------------------------------------------------------------------------

class RequestCounter {
public:
  RequestCounter(std::chrono::seconds interval);

  void account(const Transaction &transaction);
  void mainThread(ThreadAssistant &assistant);

  void setReportingStatus(bool val);
  void account(const RedisRequest &req);

  Statistics getOverallStats();

  void fillHistorical(std::vector<std::string> &headers,
    std::vector<std::vector<std::string>> &data);

private:
  void account(const RedisRequest &req, Statistics *stats);

  std::string toRate(int64_t val);
  StatAggregator aggregator;

  bool paused = true;
  std::atomic<bool> activated {true};

  std::chrono::seconds interval;
  HistoricalStatistics historical;
  AssistedThread thread;
};

}

#endif
