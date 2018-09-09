// ----------------------------------------------------------------------
// File: IntToBinaryString.hh
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

#ifndef __QUARKDB_UTILS_INT_TO_BINARY_STRING_H__
#define __QUARKDB_UTILS_INT_TO_BINARY_STRING_H__

#include "Macros.hh"

#include <endian.h>
#include <memory.h>
#include <stdint.h>
#include <string>

// Utilities to store/retrieve int64 and uint64 to binary strings.
// Big endian encoding.

namespace quarkdb {

QDB_ALWAYS_INLINE
inline int64_t binaryStringToInt(const char* buff) {
  int64_t result;
  memcpy(&result, buff, sizeof(result));
  return be64toh(result);
}

QDB_ALWAYS_INLINE
inline void intToBinaryString(int64_t num, char* buff) {
  int64_t be = htobe64(num);
  memcpy(buff, &be, sizeof(be));
}

QDB_ALWAYS_INLINE
inline std::string intToBinaryString(int64_t num) {
  char buff[sizeof(num)];
  intToBinaryString(num, buff);
  return std::string(buff, sizeof(num));
}

QDB_ALWAYS_INLINE
inline uint64_t binaryStringToUnsignedInt(const char* buff) {
  uint64_t result;
  memcpy(&result, buff, sizeof(result));
  return be64toh(result);
}

QDB_ALWAYS_INLINE
inline void unsignedIntToBinaryString(uint64_t num, char* buff) {
  uint64_t be = htobe64(num);
  memcpy(buff, &be, sizeof(be));
}

QDB_ALWAYS_INLINE
inline std::string unsignedIntToBinaryString(uint64_t num) {
  char buff[sizeof(num)];
  unsignedIntToBinaryString(num, buff);
  return std::string(buff, sizeof(num));
}

}

#endif
