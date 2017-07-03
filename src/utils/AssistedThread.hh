// ----------------------------------------------------------------------
// File: AssistedThread.hh
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
#ifndef __QUARKDB_ASSISTED_THREAD_H__
#define __QUARKDB_ASSISTED_THREAD_H__

#include <atomic>
#include <thread>

namespace quarkdb {

class AssistedThread {
public:
  // null constructor, no underlying thread
  AssistedThread() : stopFlag(true), joined(true) { }

  // universal references, perfect forwarding, variadic template
  // (C++ is intensifying)
  template<typename... Args>
  AssistedThread(Args&&... args) : stopFlag(false), joined(false), th(std::forward<Args>(args)..., std::ref(stopFlag)) {
  }

  // Only allow assignment to rvalues
  AssistedThread& operator=(const AssistedThread&) = delete;

  AssistedThread& operator=(AssistedThread&& src) noexcept {
    join();

    stopFlag = src.stopFlag.load();
    joined = src.joined.load();
    th = std::move(src.th);
    return *this;
  }

  virtual ~AssistedThread() {
    join();
  }

  void stop() {
    if(joined) return;
    stopFlag = true;
  }

  void join() {
    if(joined) return;

    stop();
    th.join();
    joined = true;
  }

private:
  std::atomic<bool> stopFlag, joined;
  std::thread th;
};

}

#endif
