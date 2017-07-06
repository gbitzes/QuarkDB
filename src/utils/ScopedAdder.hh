// ----------------------------------------------------------------------
// File: ScopedAdder.hh
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

#ifndef __QUARKDB_SCOPED_ADDER_H__
#define __QUARKDB_SCOPED_ADDER_H__

namespace quarkdb {

template<class T>
class ScopedAdder {
public:
  ScopedAdder(std::atomic<T> &target_, T value_ = 1) : target(target_), value(value_) {
    target += value;
  }

  ~ScopedAdder() {
    target -= value;
  }

private:
  std::atomic<T> &target;
  T value;
};

}


#endif
