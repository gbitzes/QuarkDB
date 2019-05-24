// ----------------------------------------------------------------------
// File: Statistics.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QUARKDB_STATISTICS_HH
#define QUARKDB_STATISTICS_HH

#include <atomic>
#include "CoreLocalArray.hh"

namespace quarkdb {

struct alignas(CoreLocal::kCacheLine) Statistics {
  Statistics() : reads(0), writes(0), txread(0), txreadwrite(0) {}

  std::atomic<int64_t> reads;
  std::atomic<int64_t> writes;
  std::atomic<int64_t> txread;
  std::atomic<int64_t> txreadwrite;

  Statistics(const Statistics &other) : reads(other.reads.load()),
  writes(other.writes.load()), txread(other.txread.load()),
  txreadwrite(other.txreadwrite.load()) {}

  Statistics& operator+=(const Statistics &other) {
    reads += other.reads;
    writes += other.writes;
    txread += other.txread;
    txreadwrite += other.txreadwrite;
    return *this;
  }

  Statistics& operator-=(const Statistics &other) {
    reads -= other.reads;
    writes -= other.writes;
    txread -= other.txread;
    txreadwrite -= other.txreadwrite;
    return *this;
  }

  Statistics& operator=(const Statistics &other) {
    reads = other.reads.load();
    writes = other.writes.load();
    txread = other.txread.load();
    txreadwrite = other.txreadwrite.load();
    return *this;
  }

};

class StatAggregator {
public:
  //----------------------------------------------------------------------------
  // Get core-local stats object for modification - never
  // decrease the given values
  //----------------------------------------------------------------------------
  Statistics* getStats();

  //----------------------------------------------------------------------------
  // Get overall statistics, since the time the server started up. Aggregation
  // over all CPU cores.
  //----------------------------------------------------------------------------
  Statistics getOverallStats();

  //----------------------------------------------------------------------------
  // Get overall statistics, but only the difference between this function was
  // called, and now.
  //----------------------------------------------------------------------------
  Statistics getOverallStatsSinceLastTime();

private:
  CoreLocalArray<Statistics> stats;
  Statistics lastTime;
};

}

#endif
