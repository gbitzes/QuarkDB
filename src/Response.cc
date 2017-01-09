//-----------------------------------------------------------------------
// File: Response.cc
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

#include "Response.hh"
#include "Utils.hh"
#include "Formatter.hh"

using namespace quarkdb;

LinkStatus Response::err(Link *link, const std::string &err) {
  return link->Send(Formatter::err(err));
}

LinkStatus Response::errArgs(Link *link, const std::string &cmd) {
  return link->Send(Formatter::errArgs(cmd));
}

LinkStatus Response::pong(Link *link) {
  return link->Send(Formatter::pong());
}

LinkStatus Response::string(Link *link, const std::string &str) {
  return link->Send(Formatter::string(str));
}

LinkStatus Response::status(Link *link, const std::string &str) {
  return link->Send(Formatter::status(str));
}

LinkStatus Response::ok(Link *link) {
  return link->Send(Formatter::ok());
}

LinkStatus Response::null(Link *link) {
  return link->Send(Formatter::null());
}

LinkStatus Response::integer(Link *link, int64_t number) {
  return link->Send(Formatter::integer(number));
}

LinkStatus Response::fromStatus(Link *link, const rocksdb::Status &status) {
  return link->Send(Formatter::fromStatus(status));
}

LinkStatus Response::vector(Link *link, const std::vector<std::string> &vec) {
  return link->Send(Formatter::vector(vec));
}

LinkStatus Response::scan(Link *link, const std::string &marker, const std::vector<std::string> &vec) {
  return link->Send(Formatter::scan(marker, vec));
}
