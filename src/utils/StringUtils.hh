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
#include <string_view>

namespace quarkdb { namespace StringUtils {

inline size_t countOccurences(std::string_view key, char c) {
  size_t ret = 0;
  for(size_t i = 0; i < key.size(); i++) {
    if(key[i] == c) {
      ret++;
    }
  }

  return ret;
}


inline std::string_view sliceToView(rocksdb::Slice slice) {
  return std::string_view(slice.data(), slice.size());
}

inline rocksdb::Slice viewToSlice(std::string_view sv) {
  return rocksdb::Slice(sv.data(), sv.size());
}

inline bool startsWith(std::string_view str, std::string_view prefix) {
  if(prefix.size() > str.size()) return false;

  for(size_t i = 0; i < prefix.size(); i++) {
    if(str[i] != prefix[i]) return false;
  }
  return true;
}

inline bool startsWithSlice(rocksdb::Slice str, rocksdb::Slice prefix) {
  return startsWith(sliceToView(str), sliceToView(prefix));
}

inline bool isPrefix(const std::string &prefix, const char *buff, size_t n) {
  if(n < prefix.size()) return false;

  for(size_t i = 0; i < prefix.size(); i++) {
    if(buff[i] != prefix[i]) return false;
  }

  return true;
}

inline bool isPrefix(const std::string &prefix, const std::string &target) {
  return isPrefix(prefix, target.c_str(), target.size());
}

inline bool isPrintable(const std::string &str) {
  for(size_t i = 0; i < str.size(); i++) {
    if(!isprint(str[i])) {
      return false;
    }
  }
  return true;
}

inline std::string escapeNonPrintable(std::string_view str) {
  std::stringstream ss;

  for(size_t i = 0; i < str.size(); i++) {
    if(isprint(str[i])) {
      ss << str[i];
    }
    else if(str[i] == '\0') {
      ss << "\\x00";
    }
    else {
      char buff[16];
      snprintf(buff, 16, "\\x%02X", (unsigned char) str[i]);
      ss << buff;
    }
  }
  return ss.str();
}

std::string base16Encode(std::string_view source);

} }

#endif
