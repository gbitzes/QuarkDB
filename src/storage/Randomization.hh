// ----------------------------------------------------------------------
// File: Randomization.hh
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

#ifndef QUARKDB_STORAGE_RANDOMIZATION_HH
#define QUARKDB_STORAGE_RANDOMIZATION_HH

#include <string_view>
#include <stdint.h>

namespace quarkdb {

using HashOutput64 = uint64_t;

// The output of this function must be completely deterministic, meaning
// the result 100% depends on the key. However, the result should be
// random-looking bytes, and never-changing, regardless of the platform,
// or endianness. We use xxhash for this.
HashOutput64 getPseudoRandomTag(std::string_view key);

}

#endif
