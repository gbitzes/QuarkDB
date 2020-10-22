// ----------------------------------------------------------------------
// File: ExpirationEventCache.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#ifndef QUARKDB_EXPIRATION_EVENT_CACHE_H
#define QUARKDB_EXPIRATION_EVENT_CACHE_H

#include <map>
#include <mutex>
#include <set>
#include <string_view>

namespace quarkdb {

using ClockValue = uint64_t;

class ExpirationEventCache {
public:
  ExpirationEventCache();

  void insert(ClockValue cl, const std::string &leaseName);
  bool empty() const;
  void remove(ClockValue cl, const std::string &leaseName);
  size_t size() const;

  ClockValue getFrontClock();
  std::string getFrontLease();
  void pop_front();


private:
  mutable std::mutex mMutex;
  std::multimap<ClockValue, std::string> mContents;
  std::set<std::string> mStoredLeases;
};

}

#endif
