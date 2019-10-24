// ----------------------------------------------------------------------
// File: TestUtils.hh
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

#ifndef __QUARKDB_TEST_UTILS_HH
#define __QUARKDB_TEST_UTILS_HH

#include <mutex>
#include <map>
#include <chrono>

class Cache {
public:
  int64_t get(const std::string &key) {
    std::scoped_lock lock(mtx);
    return earliestAcceptable[key];
  }

  void put(const std::string &key, int64_t update) {
    std::scoped_lock lock(mtx);

    int64_t current = earliestAcceptable[key];
    if(current <= update) {
      earliestAcceptable[key] = update;
    }
  }

private:
  std::mutex mtx;
  std::map<std::string, int64_t> earliestAcceptable;
};


#endif
