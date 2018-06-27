// ----------------------------------------------------------------------
// File: timekeeper.cc
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

#include "../test-reply-macros.hh"
#include "../test-utils.hh"
#include "Timekeeper.hh"
#include <iostream>

using namespace quarkdb;

TEST(Timekeeper, BasicSanity) {
  Timekeeper tk(ClockValue(123));
  ASSERT_GE(tk.getDynamicTime(), ClockValue(123));
  std::cerr << "Initialization: " << tk.getDynamicTime() << std::endl;

  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_GE(tk.getDynamicTime(), ClockValue(1123));
  std::cerr << "After 1 sec: " << tk.getDynamicTime() << std::endl;

  // Static clock should not go back in time
  ASSERT_THROW(tk.synchronize(15u), FatalException);
  ASSERT_GE(tk.getDynamicTime(), ClockValue(1123));
  std::cerr << "After unsuccessful synchronization: " << tk.getDynamicTime() << std::endl;

  // Timejump
  tk.synchronize(ClockValue(2000));
  ASSERT_GE(tk.getDynamicTime(), ClockValue(2000));
  std::cerr << "After successful synchronization at 2000 ClockValue: " << tk.getDynamicTime() << std::endl;

  // Ensure the clock doesn't go back, or something
  ClockValue prevValue = tk.getDynamicTime();

  for(size_t i = 0; i < 10; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ClockValue curVal = tk.getDynamicTime();
    ASSERT_GE(curVal, prevValue);
    prevValue = curVal;
    std::cerr << "Tick: " << prevValue << std::endl;
  }

  // Timejump which actually sets the dynamic clock back
  tk.synchronize(ClockValue(2001));
  std::cerr << "Synchronized static clock to 2001" << std::endl;

  prevValue = tk.getDynamicTime();

  for(size_t i = 0; i < 10; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ClockValue curVal = tk.getDynamicTime();
    ASSERT_GE(curVal, prevValue);
    prevValue = curVal;
    std::cerr << "Tick: " << prevValue << std::endl;
  }


}
