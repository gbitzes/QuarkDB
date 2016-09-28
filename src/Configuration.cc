// ----------------------------------------------------------------------
// File: Configuration.cc
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

#include <unistd.h>
#include <fcntl.h>

#include "XrdRedisProtocol.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "Configuration.hh"
#include "Utils.hh"

using namespace quarkdb;

bool Configuration::fromFile(const std::string &filename, Configuration &out) {
  qdb_log("Reading configuration file from " << filename);

  XrdOucEnv myEnv;
  XrdOucStream stream(&XrdRedisProtocol::eDest, getenv("XRDINSTANCE"), &myEnv, "=====> ");

  int fd;

  if ((fd = open(filename.c_str(), O_RDONLY, 0)) < 0) {
    qdb_log("config: error " << errno << " when opening " << filename);
    return false;
  }

  stream.Attach(fd);
  return Configuration::fromStream(stream, out);
}

bool Configuration::fromStream(XrdOucStream &stream, Configuration &out) {
  return false;
}
