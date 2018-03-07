// ----------------------------------------------------------------------
// File: ArrayResponseBuilder.hh
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

#ifndef QUARKDB_ARRAY_RESPONSE_BUILDER_H
#define QUARKDB_ARRAY_RESPONSE_BUILDER_H

#include <sstream>
#include "RedisEncodedResponse.hh"

namespace quarkdb {

class ArrayResponseBuilder {
public:
  ArrayResponseBuilder(size_t size, bool phantom = false);
  void push_back(const RedisEncodedResponse &item);
  RedisEncodedResponse buildResponse() const;

private:
  size_t itemsRemaining;
  bool phantom;
  std::stringstream ss;
};

}

#endif
