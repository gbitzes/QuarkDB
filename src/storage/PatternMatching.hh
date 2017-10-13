// ----------------------------------------------------------------------
// File: PatternMatching.hh
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

#ifndef __QUARKDB_PATTERN_MATCHING_H__
#define __QUARKDB_PATTERN_MATCHING_H__

namespace quarkdb {

// Given a redis pattern, extract the maximum prefix which doesn't contain
// special glob-style characters. The rationale behind this is that we can
// efficiently rule out all keys inside RocksDB which don't start with this
// prefix.

inline std::string extractPatternPrefix(const std::string &pattern) {
  for(size_t i = 0; i < pattern.size(); i++) {
    char c = pattern[i];

    if(c == '?' || c == '*' || c == '[' || c == ']' || c == '\\') {
      return std::string(pattern.begin(), pattern.begin()+i);
    }
  }

  // The entire thing doesn't contain special characters.
  // Kinda weird, as it can only match a single key, but possible
  return pattern;
}

}

#endif
