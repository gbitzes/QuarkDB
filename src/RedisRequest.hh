// ----------------------------------------------------------------------
// File: RedisRequest.hh
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

#ifndef __QUARKDB_REDIS_REQUEST_H__
#define __QUARKDB_REDIS_REQUEST_H__

#include <string>
#include <vector>

namespace quarkdb {

class RedisRequest {
public:
  using container = std::vector<std::string>;
  using iterator = container::iterator;
  using const_iterator = container::const_iterator;

  RedisRequest(std::initializer_list<std::string> list) {
    for(auto it = list.begin(); it != list.end(); it++) {
      contents.push_back(*it);
    }
  }

  RedisRequest() {}
  RedisRequest(const std::vector<std::string> &init) {
    for(size_t i = 0; i < init.size(); i++) {
      contents.emplace_back(init[i]);
    }
  }

  size_t size() const {
    return contents.size();
  }

  std::string& operator[](size_t i) {
    return contents[i];
  }

  const std::string& operator[](size_t i) const {
    return contents[i];
  }

  bool operator==(const RedisRequest &rhs) const {
    return contents == rhs.contents;
  }

  void clear() {
    contents.clear();
  }

  void emplace_back(std::string &&src) {
    contents.emplace_back(std::move(src));
  }

  void emplace_back(const char* buf, size_t size) {
    contents.emplace_back(buf, size);
  }

  iterator begin() {
    return contents.begin();
  }

  iterator end() {
    return contents.end();
  }

  const_iterator begin() const {
    return contents.begin();
  }

  const_iterator end() const {
    return contents.end();
  }

  void reserve(size_t size) {
    contents.reserve(size);
  }
private:
  std::vector<std::string> contents;
};

}

#endif
