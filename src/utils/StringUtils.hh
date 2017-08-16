// ----------------------------------------------------------------------
// File: StringUtils.hh
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

#ifndef __QUARKDB_UTILS_STRING_UTILS_H__
#define __QUARKDB_UTILS_STRING_UTILS_H__

#include <rocksdb/slice.h>
#include <string>

namespace quarkdb { namespace StringUtils {

inline size_t countOccurences(const std::string &key, char c) {
  size_t ret = 0;
  for(size_t i = 0; i < key.size(); i++) {
    if(key[i] == c) {
      ret++;
    }
  }

  return ret;
}

inline bool startswith(const std::string &str, const rocksdb::Slice &prefix) {
  if(prefix.size() > str.size()) return false;

  for(size_t i = 0; i < prefix.size(); i++) {
    if(str[i] != prefix[i]) return false;
  }
  return true;
}

} }

#endif
