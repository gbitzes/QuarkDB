// ----------------------------------------------------------------------
// File: ExpirationEventCache.cc
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

#include "storage/ExpirationEventCache.hh"
#include "utils/Macros.hh"

using namespace quarkdb;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ExpirationEventCache::ExpirationEventCache() {}

//------------------------------------------------------------------------------
// Insert expiration event
//------------------------------------------------------------------------------
void ExpirationEventCache::insert(ClockValue cl, const std::string &leaseName) {
  std::scoped_lock lock(mMutex);

  qdb_assert(mStoredLeases.count(leaseName) == 0u);
  mContents.emplace(cl, leaseName);
  mStoredLeases.insert(leaseName);
}

//------------------------------------------------------------------------------
// Check if empty
//------------------------------------------------------------------------------
bool ExpirationEventCache::empty() const {
  std::scoped_lock lock(mMutex);
  return mContents.empty();
}

//------------------------------------------------------------------------------
// Get clock value in the front item
//------------------------------------------------------------------------------
ClockValue ExpirationEventCache::getFrontClock() {
  std::scoped_lock lock(mMutex);
  return mContents.begin()->first;
}

//------------------------------------------------------------------------------
// Get lease name in the front item
//------------------------------------------------------------------------------
std::string ExpirationEventCache::getFrontLease() {
  std::scoped_lock lock(mMutex);
  return mContents.begin()->second;
}

//------------------------------------------------------------------------------
// Erase front item
//------------------------------------------------------------------------------
void ExpirationEventCache::pop_front() {
  std::scoped_lock lock(mMutex);
  qdb_assert(!mContents.empty());
  qdb_assert(mStoredLeases.erase(mContents.begin()->second) == 1u);
  mContents.erase(mContents.begin());
}

//------------------------------------------------------------------------------
// Erase given item
//------------------------------------------------------------------------------
void ExpirationEventCache::remove(ClockValue cl, const std::string &leaseName) {
  std::scoped_lock lock(mMutex);

  auto it = mContents.find(cl);
  
  while(it != mContents.end() && it->first == cl) {
    if(it->second == leaseName) {
      mContents.erase(it);
      qdb_assert(mStoredLeases.erase(leaseName) == 1u);
      return;
    }

    it++;
  }

  qdb_throw("unable to find lease to remove: " << cl << ", " << leaseName);
}

//------------------------------------------------------------------------------
// Lookup map size
//------------------------------------------------------------------------------
size_t ExpirationEventCache::size() const {
  std::scoped_lock lock(mMutex);
  return mContents.size();
}
