// ----------------------------------------------------------------------
// File: ClockWrapper.hh
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

#ifndef QUARKDB_CLOCK_WRAPPER_HH
#define QUARKDB_CLOCK_WRAPPER_HH

#include <chrono>
#include <mutex>

namespace quarkdb {

//------------------------------------------------------------------------------
// A template to wrap any std::clock and provide faking abilities. In a fake
// clock, time only changes when asked explicitly with advance() or set().
//
// - A faked steady clock can only be advanced.
// - A faked non-steady clock can be set to any value.
//
// If faking is de-activated, now() dispatches to the underlying type's
// T::now().
//
// Example usage:
//   SteadyClock realClock;
//   realClock.advance( .. ) --> has no effects
//   realClock.now() --> std::chrono::steady_clock::now()
//
//   SteadyClock fakeClock(true);
//   fakeClock.advance(std::chrono::seconds(1));
//   fakeClock.now() --> 1 seconds from the beginning of time, which is
//                       indicated as a default-constructed T::time_point.
//------------------------------------------------------------------------------
template<typename T>
class ClockWrapper {
public:
  //----------------------------------------------------------------------------
  // Forward underlying clock's definitions for rep, period, duration, and
  // time_point.
  //----------------------------------------------------------------------------
  using rep = typename T::rep;
  using period = typename T::period;
  using duration = typename T::duration;
  using time_point = typename T::time_point;

  //----------------------------------------------------------------------------
  // Forward is_steady
  //----------------------------------------------------------------------------
  static constexpr bool is_steady = T::is_steady;

  //----------------------------------------------------------------------------
  // Constructor - specify if we're faking time, or not.
  // When faking time, we're starting from a defult-constructed time_point.
  //----------------------------------------------------------------------------
  ClockWrapper(bool fake = false) : faking(fake) {}

  //----------------------------------------------------------------------------
  // Are we faking time?
  //----------------------------------------------------------------------------
  bool fake() const {
    return faking;
  }

  //----------------------------------------------------------------------------
  // Get current time.
  //----------------------------------------------------------------------------
  time_point now() const {
    if(faking) {
      std::unique_lock<std::mutex> lock(mtx);
      return fakeTime;
    }

    return T::now();
  }

  //----------------------------------------------------------------------------
  // Advance time - available in all clocks. Has no effect on now() if we
  // aren't faking time in this object.
  //----------------------------------------------------------------------------
  template<typename Dur>
  void advance(Dur duration) {
    std::unique_lock<std::mutex> lock(mtx);
    fakeTime += duration;
  }

  //----------------------------------------------------------------------------
  // Set time to specified timepoint - available only in non-steady clocks.
  // For steady-clocks, use advance.
  //
  // Has no effect on now() if we aren't faking time in this object.
  //----------------------------------------------------------------------------
  std::enable_if_t<std::is_same_v<
    std::false_type,
    std::integral_constant<bool, T::is_steady>
  >, void>
  set(time_point point) {
    std::unique_lock<std::mutex> lock(mtx);
    fakeTime = point;
  }

private:
  bool faking;
  mutable std::mutex mtx;
  time_point fakeTime;
};

using SteadyClock = ClockWrapper<std::chrono::steady_clock>;
using SystemClock = ClockWrapper<std::chrono::system_clock>;

}

#endif
