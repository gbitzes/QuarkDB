// ----------------------------------------------------------------------
// File: Timekeeper.cc
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

#include "Timekeeper.hh"
#include "utils/Macros.hh"
using namespace quarkdb;

Timekeeper::Timekeeper(ClockValue startup) : staticClock(startup) {
  anchorPoint = std::chrono::steady_clock::now();
}

void Timekeeper::reset(ClockValue startup) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  staticClock = startup;
  anchorPoint = std::chrono::steady_clock::now();
}

void Timekeeper::synchronize(ClockValue newval) {
  std::unique_lock<std::shared_mutex> lock(mtx);
  qdb_assert(staticClock <= newval);

  // We have a timejump. Re-anchor, update static clock
  anchorPoint = std::chrono::steady_clock::now();
  staticClock = newval;
}

ClockValue Timekeeper::getDynamicTime() const {
  std::shared_lock<std::shared_mutex> lock(mtx);
  return staticClock + getTimeSinceAnchor().count();
}

std::chrono::milliseconds Timekeeper::getTimeSinceAnchor() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - anchorPoint
  );
}
