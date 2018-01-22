// ----------------------------------------------------------------------
// File: Random.cc
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

#include "Random.hh"
#include "Macros.hh"
#include <stdio.h>

namespace quarkdb {

std::string generateSecureRandomBytes(size_t nbytes) {
  char buffer[nbytes + 1];

  // We might want to keep a pool of open "/dev/urandom" on standby, to avoid
  // opening and closing /dev/urandom too often, but meh, this'll do for now.

  FILE *in = fopen("/dev/urandom", "rb");

  if(!in) qdb_throw("unable to open /dev/urandom");

  size_t bytes_read = fread(buffer, 1, nbytes, in);
  qdb_assert(bytes_read == nbytes);
  qdb_assert(fclose(in) == 0);

  return std::string(buffer, nbytes);
}

}
