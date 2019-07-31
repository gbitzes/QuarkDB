// ----------------------------------------------------------------------
// File: ThreadSafeString.hh
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

#ifndef QUARKDB_THREAD_SAFE_STRING_HH
#define QUARKDB_THREAD_SAFE_STRING_HH

#include <string>
#include <shared_mutex>

namespace quarkdb {

class ThreadSafeString {
public:
  ThreadSafeString(const std::string &val) : contents(val) {}
  ThreadSafeString() {}

  void set(const std::string &value) {
    std::unique_lock<std::shared_mutex> lock(mtx);
    contents = value;
  }

  std::string get() const {
    std::shared_lock<std::shared_mutex> lock(mtx);
    return contents;
  }

private:
  mutable std::shared_mutex mtx;
  std::string contents;
};

}


#endif
