// ----------------------------------------------------------------------
// File: SubscriptionTracker.hh
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

#ifndef QUARKDB_PUBSUB_SUBSCRIPTION_TRACKER_HH
#define QUARKDB_PUBSUB_SUBSCRIPTION_TRACKER_HH

#include <string>
#include <set>

namespace quarkdb {

//------------------------------------------------------------------------------
// Simple class to keep track of the pubsub state of a particular connection.
// Not thread-safe, need to synchronize externally.
//------------------------------------------------------------------------------
class SubscriptionTracker {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  SubscriptionTracker() {}

  //----------------------------------------------------------------------------
  // Add channel
  //----------------------------------------------------------------------------
  bool addChannel(const std::string &item);

  //----------------------------------------------------------------------------
  // Add pattern
  //----------------------------------------------------------------------------
  bool addPattern(const std::string &item);

  //----------------------------------------------------------------------------
  // Check for existence of a channel
  //----------------------------------------------------------------------------
  bool hasChannel(const std::string &item) const;

  //----------------------------------------------------------------------------
  // Check for existence of a pattern
  //----------------------------------------------------------------------------
  bool hasPattern(const std::string &item) const;

private:
  std::set<std::string> channels;
  std::set<std::string> patterns;
};


}

#endif