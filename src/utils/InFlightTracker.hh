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
#include "Macros.hh"
#include "CoreLocalArray.hh"

namespace quarkdb {

struct alignas(CoreLocal::kCacheLine) AlignedAtomicInt64_t {
  std::atomic<int64_t> value = {0};
};

//------------------------------------------------------------------------------
// Keep track of how many requests are currently in-flight.
// It's also possible to use this as a barrier to further requests - useful
// when shutting down.
//------------------------------------------------------------------------------

class InFlightTracker {
public:
  InFlightTracker(bool accepting = true) : acceptingRequests(accepting) {}

  int up() {
    // This contraption (hopefully) ensures that after setAcceptingRequests(false)
    // takes effect, the following guarantees hold:
    // - Any subsequent calls to up() will not increase inFlight.
    // - As soon as we observe an inFlight value of zero, no further requests
    //   will be accepted.
    //
    // The second guarantee is necessary for wait(), which checks if inFlight
    // is zero to tell whether all in-flight requests have been dispatched.

    // If setAcceptingRequests takes effect here, the request is rejected, as expected.
    if(!acceptingRequests) return -1;

    // If setAcceptingRequests takes effect here, no problem. inFlight will
    // temporarily jump, but the request will be rejected.
    int coreIdx = inFlightArr.getCoreIndex();
    inFlightArr.accessAtCore(coreIdx)->value++;

    // Same as before.
    if(!acceptingRequests) {
      // If we're here, it means setAcceptingRequests has already taken effect.
      inFlightArr.accessAtCore(coreIdx)->value--;
      return -1;
    }

    // If setAcceptingRequests takes effect here, no problem:
    // inFlight can NOT be zero at this point, and the spinner will wait.

    return coreIdx;
  }

  void down(int coreIdx) {
    inFlightArr.accessAtCore(coreIdx)->value--;
    qdb_assert(inFlightArr.accessAtCore(coreIdx)->value >= 0);
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
    int64_t inFlight = 0;
    for(size_t i = 0; i < inFlightArr.size(); i++) {
      inFlight += inFlightArr.accessAtCore(i)->value;
    }

    return inFlight;
  }

private:
  std::atomic<bool> acceptingRequests {true};
  CoreLocalArray<AlignedAtomicInt64_t> inFlightArr;
};

class InFlightRegistration {
public:
  InFlightRegistration(InFlightTracker &tracker) : inFlightTracker(tracker) {
    coreIdx = inFlightTracker.up();
  }

  ~InFlightRegistration() {
    if(ok()) {
      inFlightTracker.down(coreIdx);
    }
  }

  bool ok() {
    return coreIdx >= 0;
  }

private:
  InFlightTracker &inFlightTracker;
  int coreIdx;
  bool succeeded;
};

}

#endif
