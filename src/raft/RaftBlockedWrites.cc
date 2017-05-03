// ----------------------------------------------------------------------
// File: RaftBlockedWrites.cc
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

#include "RaftBlockedWrites.hh"
#include "../Connection.hh"
using namespace quarkdb;

std::shared_ptr<PendingQueue> RaftBlockedWrites::popIndex(LogIndex index) {
  std::lock_guard<std::mutex> lock(mtx);
  auto it = tracker.find(index);

  if(it == tracker.end()) return {nullptr};

  std::shared_ptr<PendingQueue> ret = it->second;
  tracker.erase(it);

  return ret;
}

void RaftBlockedWrites::insert(LogIndex index, const std::shared_ptr<PendingQueue> &item) {
  std::lock_guard<std::mutex> lock(mtx);
  tracker[index] = item;
}

void RaftBlockedWrites::flush(const std::string &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  for(auto it = tracker.begin(); it != tracker.end(); it++) {
    it->second->flushPending(msg);
  }
  tracker.clear();
}

size_t RaftBlockedWrites::size() {
  std::lock_guard<std::mutex> lock(mtx);
  return tracker.size();
}
