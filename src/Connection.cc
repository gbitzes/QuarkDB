//----------------------------------------------------------------------
// File: Connection.hh
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

#include "Connection.hh"
using namespace quarkdb;

Connection::Connection(Link *l)
: link(l) {
}

Connection::~Connection() {
}


LinkStatus Connection::err(const std::string &msg) {
  return Response::err(link, msg);
}

LinkStatus Connection::errArgs(const std::string &cmd) {
  return Response::errArgs(link, cmd);
}

LinkStatus Connection::pong() {
  return Response::pong(link);
}

LinkStatus Connection::string(const std::string &str) {
  return Response::string(link, str);
}

LinkStatus Connection::fromStatus(const rocksdb::Status &status) {
  return Response::fromStatus(link, status);
}

LinkStatus Connection::ok() {
  return Response::ok(link);
}

LinkStatus Connection::null() {
  return Response::null(link);
}

LinkStatus Connection::integer(int64_t number) {
  return Response::integer(link, number);
}

LinkStatus Connection::vector(const std::vector<std::string> &vec) {
  return Response::vector(link, vec);
}

LinkStatus Connection::scan(const std::string &marker, const std::vector<std::string> &vec) {
  return Response::scan(link, marker, vec);
}
