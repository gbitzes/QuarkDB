// ----------------------------------------------------------------------
// File: Synchronized.hh
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

#ifndef QUARKDB_SYNCHRONIZED_HH
#define QUARKDB_SYNCHRONIZED_HH

#include <shared_mutex>

namespace quarkdb {

template<typename T>
class Synchronized {
public:
  Synchronized(const T& t) {}

  Synchronized() {}

  template<typename... Args>
  void set(Args&& ... args) {
    std::unique_lock<std::shared_mutex> lock(mtx);
    contents = std::string(std::forward<Args>(args)...);
  }

  T get() const {
    std::shared_lock<std::shared_mutex> lock(mtx);
    return contents;
  }

private:
  T contents;
  mutable std::shared_mutex mtx;
};

}

#endif
