// ----------------------------------------------------------------------
// File: Randomization.cc
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

#include "Randomization.hh"
#include "../../deps/xxhash/xxhash.hh"

namespace quarkdb {

HashOutput64 getPseudoRandomTag(std::string_view key) {
  // Changing the seed will corrupt all existing fields which depend on
  // random tags. Don't do it. :)
  constexpr uint64_t kSeed = 0x1d7a6b7d;
  return XXH64(key.data(), key.size(), kSeed);
}

}
