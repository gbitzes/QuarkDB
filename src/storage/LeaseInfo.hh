// ----------------------------------------------------------------------
// File: LeaseInfo.hh
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

#ifndef QUARKDB_LEASE_INFO_H
#define QUARKDB_LEASE_INFO_H

#include <string_view>

namespace quarkdb {

using ClockValue = uint64_t;

class LeaseInfo {
public:
  LeaseInfo() {}

  LeaseInfo(std::string_view val, ClockValue ren, ClockValue dl) :
    value(val), lastRenewal(ren), deadline(dl) {}

  ClockValue getLastRenewal() const {
    return lastRenewal;
  }

  ClockValue getDeadline() const {
    return deadline;
  }

  std::string_view getValue() const {
    return value;
  }

private:
  std::string value;
  ClockValue lastRenewal;
  ClockValue deadline;
};

}
#endif
