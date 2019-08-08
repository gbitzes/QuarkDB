// ----------------------------------------------------------------------
// File: StaticBuffer.hh
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

#ifndef QUARKDB_UTILS_STATIC_BUFFER_HH
#define QUARKDB_UTILS_STATIC_BUFFER_HH

#include "Utils.hh"
#include <rocksdb/slice.h>

namespace quarkdb {

template<int static_size>
class StaticBuffer {
public:
  StaticBuffer() {}

  char* data() {
    return contents;
  }

  size_t size() {
    return runtime_size;
  }

  std::string_view toView() {
    return std::string_view(data(), size());
  }

  void shrink(size_t newsize) {
    qdb_assert(newsize <= static_size);
    runtime_size = newsize;
  }

  char& operator[](size_t i) {
    return contents[i];
  }

private:
  char contents[static_size];
  size_t runtime_size = static_size;
};


}

#endif
