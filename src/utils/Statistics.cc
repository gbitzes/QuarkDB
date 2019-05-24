// ----------------------------------------------------------------------
// File: Statistics.cc
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

#include "Statistics.hh"
#include "Macros.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Describe contents as a vector
//------------------------------------------------------------------------------
std::vector<std::string> Statistics::serialize() const {
  std::vector<std::string> output(4);
  output[0] = SSTR("READS " << reads.load());
  output[1] = SSTR("WRITES " << writes.load());
  output[2] = SSTR("TXREAD " << txread.load());
  output[3] = SSTR("TXREADWRITE " << txreadwrite.load());
  return output;
}

//------------------------------------------------------------------------------
// Push new datapoint, along with corresponding timestamp
//------------------------------------------------------------------------------
void HistoricalStatistics::push(const Statistics &stats,
  std::chrono::steady_clock::time_point point) {

  std::lock_guard<std::mutex> lock(mtx);
  store.emplace_front(point, stats);

  if(store.size() > retentionLimit) {
    store.pop_back();
  }
}

//------------------------------------------------------------------------------
// Export into vector-of-vectors-with-headers format
//------------------------------------------------------------------------------
void HistoricalStatistics::serialize(std::vector<std::string> &headers,
    std::vector<std::vector<std::string>> &data) {

  std::lock_guard<std::mutex> lock(mtx);

  headers.resize(store.size());
  data.resize(store.size());

  size_t i = 0;
  for(auto it = store.begin(); it != store.end(); it++) {
    headers[i] = SSTR("TIMESTAMP " <<
      std::chrono::duration_cast<std::chrono::seconds>(it->timepoint.time_since_epoch()).count());
    data[i] = it->stats.serialize();

    i++;
  }
}

//------------------------------------------------------------------------------
// Get core-local stats object for modification - never
// decrease the given values
//------------------------------------------------------------------------------
Statistics* StatAggregator::getStats() {
  return stats.access().first;
}

//------------------------------------------------------------------------------
// Get overall statistics, since the time the server started up. Aggregation
// over all CPU cores.
//------------------------------------------------------------------------------
Statistics StatAggregator::getOverallStats() {
  Statistics output;

  for(size_t i = 0; i < stats.size(); i++) {
    output += *(stats.accessAtCore(i));
  }

  return output;
}

//------------------------------------------------------------------------------
// Get overall statistics, but only the difference between this function was
// called, and now.
//------------------------------------------------------------------------------
Statistics StatAggregator::getOverallStatsSinceLastTime() {
  Statistics overAllNow = getOverallStats();

  Statistics output = overAllNow;
  output -= lastTime;

  lastTime = overAllNow;
  return output;
}


}
