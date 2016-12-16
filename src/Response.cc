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

using namespace quarkdb;

LinkStatus Response::err(Link *link, const std::string &err) {
  if(!startswith(err, "ERR ") && !startswith(err, "MOVED ")) qdb_throw("invalid error message: " << err);
  return link->Send(SSTR("-" << err << "\r\n"));
}

LinkStatus Response::errArgs(Link *link, const std::string &cmd) {
  return link->Send(SSTR("-ERR wrong number of arguments for '" << cmd << "' command\r\n"));
}

LinkStatus Response::pong(Link *link) {
  return link->Send("+PONG\r\n");
}

LinkStatus Response::string(Link *link, const std::string &str) {
  return link->Send(SSTR("$" << str.length() << "\r\n" << str << "\r\n"));
}

LinkStatus Response::status(Link *link, const std::string &str) {
  return link->Send(SSTR("+" << str << "\r\n"));
}

LinkStatus Response::ok(Link *link) {
  return link->Send("+OK\r\n");
}

LinkStatus Response::null(Link *link) {
  return link->Send("$-1\r\n");
}

LinkStatus Response::integer(Link *link, int64_t number) {
  return link->Send(SSTR(":" << number << "\r\n"));
}

LinkStatus Response::fromStatus(Link *link, const rocksdb::Status &status) {
  if(status.ok()) return Response::ok(link);
  return Response::err(link, status.ToString());
}

LinkStatus Response::vector(Link *link, const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return link->Send(ss.str());
}

LinkStatus Response::scan(Link *link, const std::string &marker, const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*2\r\n";
  ss << "$" << marker.length() << "\r\n";
  ss << marker << "\r\n";

  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return link->Send(ss.str());
}
