// ----------------------------------------------------------------------
// File: MultiOp.hh
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

#ifndef QUARKDB_REDIS_MULTIOP_H
#define QUARKDB_REDIS_MULTIOP_H

#include "../RedisRequest.hh"

namespace quarkdb {

class MultiOp {
public:
  MultiOp();
  ~MultiOp();

  void push_back(const RedisRequest &req);
  bool containsWrites() const {
    return hasWrites;
  }

  std::string serialize() const;
  bool deserialize(const std::string &src);

  RedisRequest& operator[](size_t i) {
    return requests[i];
  }

  const RedisRequest& operator[](size_t i) const {
    return requests[i];
  }

  bool operator==(const MultiOp &rhs) const {
    return requests == rhs.requests;
  }

  template<typename... Args>
  void emplace_back(Args&&... args) {
    requests.push_back( RedisRequest { args ... } );
    checkLastCommandForWrites();
  }

  size_t size() const {
    return requests.size();
  }

  bool empty() const {
    return (requests.size() == 0u);
  }

  void clear();

  RedisRequest toRedisRequest(bool phantom) const;
  std::string getFusedCommand() const;

private:
  void checkLastCommandForWrites();

  bool hasWrites = false;
  std::vector<RedisRequest> requests;
};

}

#endif
