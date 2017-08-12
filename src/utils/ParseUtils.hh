// ----------------------------------------------------------------------
// File: ParsingUtils.hh
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

#ifndef __QUARKDB_PARSING_UTILS_H__
#define __QUARKDB_PARSING_UTILS_H__

#include "../Utils.hh"

#include <climits>

namespace quarkdb { namespace ParseUtils {

inline std::vector<std::string> split(std::string data, const std::string &token) {
  std::vector<std::string> output;
  size_t pos = std::string::npos;
  do {
    pos = data.find(token);
    output.push_back(data.substr(0, pos));
    if(std::string::npos != pos) {
      data = data.substr(pos + token.size());
    }
  } while (std::string::npos != pos);
  return output;
}

inline bool parseInteger(const std::string &str, int64_t &ret) {
  char *endptr = NULL;
  ret = strtoll(str.c_str(), &endptr, 10);
  if(endptr != str.c_str() + str.size() || ret == LLONG_MIN || ret == LONG_LONG_MAX) {
    return false;
  }
  return true;
}

inline bool parseIntegerList(const std::string &buffer, std::vector<int64_t> &results) {
  results.clear();

  std::vector<std::string> items = ParseUtils::split(buffer, ",");

  for(size_t i = 0; i < items.size(); i++) {
    int64_t value;
    if(!ParseUtils::parseInteger(items[i], value)) {
      return false;
    }

    results.emplace_back(value);
  }

  return true;
}

} }

#endif
