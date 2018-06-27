// ----------------------------------------------------------------------
// File: Timekeeper.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#ifndef QUARKDB_TIME_KEEPER_H
#define QUARKDB_TIME_KEEPER_H

#include <chrono>
#include <shared_mutex>

namespace quarkdb {

using ClockValue = uint64_t;

class Timekeeper {
public:
  //----------------------------------------------------------------------------
  // Construct Timekeeper with the given initial ClockValue. Time starts
  // rolling forward as soon as the object is constructed.
  //----------------------------------------------------------------------------
  Timekeeper(ClockValue startup);

  //----------------------------------------------------------------------------
  // The static clock has been updated to the given value. Most of the time,
  // the given value should actually be less than what we have. If that's not
  // the case, update.
  //
  // A Timekeeper will never go back in time!
  //----------------------------------------------------------------------------
  bool synchronize(ClockValue observed);

  //----------------------------------------------------------------------------
  // Get the current time in milliseconds.
  //----------------------------------------------------------------------------
  ClockValue getCurrentTime();

private:
  std::shared_mutex mtx;
  ClockValue staticClock;
  std::chrono::steady_clock::time_point anchorPoint;

  //----------------------------------------------------------------------------
  // Get time elapsed since last anchor point
  //----------------------------------------------------------------------------
  std::chrono::milliseconds getTimeSinceAnchor();
};

}

#endif
