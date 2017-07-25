// ----------------------------------------------------------------------
// File: InFlightTracker.hh
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

#ifndef __QUARKDB_IN_FLIGHT_TRACKER_H__
#define __QUARKDB_IN_FLIGHT_TRACKER_H__

#include <atomic>
#include "../Utils.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Keep track of how many requests are currently in-flight.
// It's also possible to use this as a barrier to further requests - useful
// when shutting down.
//------------------------------------------------------------------------------

class InFlightTracker {
public:
  InFlightTracker(bool accepting = true) : acceptingRequests(accepting) {}

  bool up() {
    // This contraption (hopefully) ensures that after setAcceptingRequests(false)
    // takes effect, the following guarantees hold:
    // - Any subsequent calls to up() will not increase inFlight.
    // - As soon as we observe an inFlight value of zero, no further requests
    //   will be accepted.
    //
    // The second guarantee is necessary for wait(), which checks if inFlight
    // is zero to tell whether all in-flight requests have been dispatched.

    // If setAcceptingRequests takes effect here, the request is rejected, as expected.
    if(!acceptingRequests) return false;

    // If setAcceptingRequests takes effect here, no problem. inFlight will
    // temporarily jump, but the request will be rejected.

    inFlight++;

    // Same as before.
    if(!acceptingRequests) {
      // If we're here, it means setAcceptingRequests has already taken effect.
      inFlight--;
      return false;
    }

    // If setAcceptingRequests takes effect here, no problem:
    // inFlight can NOT be zero at this point, and the spinner will wait.

    return true;
  }

  void down() {
    inFlight--;
    qdb_assert(inFlight >= 0);
  }

  void setAcceptingRequests(bool value) {
    acceptingRequests = value;
  }

  bool isAcceptingRequests() const {
    return acceptingRequests;
  }

  void spinUntilNoRequestsInFlight() const {
    qdb_assert(!acceptingRequests);
    while(getInFlight() != 0) ;
  }

  int64_t getInFlight() const {
    return inFlight;
  }

private:
  std::atomic<bool> acceptingRequests {true};
  std::atomic<int64_t> inFlight {0};
};

class InFlightRegistration {
public:
  InFlightRegistration(InFlightTracker &tracker) : inFlightTracker(tracker) {
    succeeded = inFlightTracker.up();
  }

  ~InFlightRegistration() {
    if(succeeded) {
      inFlightTracker.down();
    }
  }

  bool ok() {
    return succeeded;
  }

private:
  InFlightTracker &inFlightTracker;
  bool succeeded;
};

}

#endif
