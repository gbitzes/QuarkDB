//-----------------------------------------------------------------------
// File: RedisEncodedResponse.hh
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

#ifndef QUARKDB_REDIS_REDISENCODEDRESPONSE_H
#define QUARKDB_REDIS_REDISENCODEDRESPONSE_H

#include <string>

namespace quarkdb {

// Phantom type: std::string with a special meaning. Unless explicitly asked
// with obj.val, this will generate compiler errors when you try to use like
// plain string.
class RedisEncodedResponse {
public:
  explicit RedisEncodedResponse(std::string &&src) : val(std::move(src)) {}
  RedisEncodedResponse() {}
  bool empty() const { return val.empty(); }
  std::string val;
};

}

#endif
