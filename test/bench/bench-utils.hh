// ----------------------------------------------------------------------
// File: bench-utils.hh
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

#ifndef __QUARKDB_BENCH_UTILS_H__
#define __QUARKDB_BENCH_UTILS_H__

#include <chrono>

namespace quarkdb {

class Stopwatch {
public:
  Stopwatch(size_t events)
  : nevents(events) {
    start();
  }

  void start() {
    startTime = std::chrono::high_resolution_clock::now();
  }

  void stop() {
    endTime = std::chrono::high_resolution_clock::now();
  }

  // returns rate in Hz
  float rate() {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    return (float) nevents /  ((float) duration / (float) 1000);
  }

private:
  size_t nevents;
  std::chrono::high_resolution_clock::time_point startTime;
  std::chrono::high_resolution_clock::time_point endTime;
};

}

#endif
