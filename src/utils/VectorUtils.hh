// ----------------------------------------------------------------------
// File: VectorUtils.hh
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

#ifndef __QUARKDB_VECTOR_UTILS_H__
#define __QUARKDB_VECTOR_UTILS_H__

namespace quarkdb { namespace VectorUtils {

template<typename T>
bool checkEquality(const T& one, const T& two) {
  if(one.size() != two.size()) return false;

  for(size_t i = 0; i < one.size(); i++) {
    // Using != would be more natural here, but most classes
    // only define operator==
    if(one[i] == two[i]) continue;
    return false;
  }

  return true;
}

} }



#endif
