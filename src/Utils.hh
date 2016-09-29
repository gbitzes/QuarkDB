// ----------------------------------------------------------------------
// File: Utils.hh
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

#ifndef __QUARKDB_UTILS_H__
#define __QUARKDB_UTILS_H__

#include <iostream>
#include <sstream>
#include <vector>
#include <set>

#include "Common.hh"

namespace quarkdb {

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define quotes(message) SSTR("'" << message << "'")


// temporary solution for now
#define qdb_log(message) std::cerr << message << std::endl;
#define qdb_critical(message) std::cerr << message << std::endl;

bool my_strtoll(const std::string &str, int64_t &ret);
std::vector<std::string> split(std::string data, std::string token);
bool parseServer(const std::string &str, RaftServer &srv);
bool parseServers(const std::string &str, std::vector<RaftServer> &servers);

// given a vector, checks whether all elements are unique
template<class T>
bool checkUnique(std::vector<T> &v) {
  for(size_t i = 0; i < v.size(); i++) {
    for(size_t j = 0; j < v.size(); j++) {
      if(i != j && v[i] == v[j]) {
        return false;
      }
    }
  }
  return true;
}

// given two iterators, checks whether all elements between them are unique
// template<class Iter>
// bool checkUnique(Iter &&begin, Iter &&end) {
//   using T = typename Iter::value_type;
//   std::unordered_set<T> unique;
//   size_t distance = 0;
//   for(auto it = begin; it != end; it++) {
//     unique.insert(*it);
//   }
//
//   return unique.size() == distance;
// }

}

#endif
