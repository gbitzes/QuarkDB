// ----------------------------------------------------------------------
// File: ArrayResponseBuilder.cc
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

#include "utils/Macros.hh"
#include "redis/ArrayResponseBuilder.hh"
using namespace quarkdb;

ArrayResponseBuilder::ArrayResponseBuilder(size_t size, bool phant)
: itemsRemaining(size), phantom(phant) {
  qdb_assert(itemsRemaining >= 1);

  if(!phantom) {
    ss << "*" << size << "\r\n";
  }
}

void ArrayResponseBuilder::push_back(const RedisEncodedResponse &item) {
  qdb_assert(itemsRemaining != 0);
  itemsRemaining--;

  ss << item.val;
}

RedisEncodedResponse ArrayResponseBuilder::buildResponse() const {
  qdb_assert(itemsRemaining == 0);
  return RedisEncodedResponse(ss.str());
}
