// ----------------------------------------------------------------------
// File: clock-wrapper.cc
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

#include "utils/ClockWrapper.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(ClockWrapper, Steady) {
  SteadyClock steadyClock(true);
  ASSERT_TRUE(steadyClock.fake());
  ASSERT_EQ(steadyClock.now(), SteadyClock::time_point());
  ASSERT_TRUE(SteadyClock::is_steady);

  SteadyClock::time_point startOfTime;
  steadyClock.advance(std::chrono::seconds(5));
  ASSERT_EQ(steadyClock.now(), startOfTime+std::chrono::seconds(5));

  steadyClock.advance(std::chrono::seconds(10));
  ASSERT_EQ(steadyClock.now(), startOfTime+std::chrono::seconds(15));
}

TEST(ClockWrapper, System) {
  SystemClock systemClock(true);
  ASSERT_TRUE(systemClock.fake());

  ASSERT_EQ(systemClock.now(), SystemClock::time_point());
  ASSERT_FALSE(SystemClock::is_steady);

  SystemClock::time_point startOfTime;
  systemClock.advance(std::chrono::seconds(5));
  ASSERT_EQ(systemClock.now(), startOfTime+std::chrono::seconds(5));

  systemClock.advance(std::chrono::seconds(10));
  ASSERT_EQ(systemClock.now(), startOfTime+std::chrono::seconds(15));

  systemClock.set(startOfTime+std::chrono::seconds(1)); // go backwards
  ASSERT_EQ(systemClock.now(), startOfTime+std::chrono::seconds(1));
}
