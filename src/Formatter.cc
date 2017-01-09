//-----------------------------------------------------------------------
// File: Formatter.cc
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

#include "Formatter.hh"
using namespace quarkdb;

std::string Formatter::err(const std::string &err) {
  if(!startswith(err, "ERR ") && !startswith(err, "MOVED ")) qdb_throw("invalid error message: " << err);
  return SSTR("-" << err << "\r\n");
}

std::string Formatter::errArgs(const std::string &cmd) {
  return SSTR("-ERR wrong number of arguments for '" << cmd << "' command\r\n");
}

std::string Formatter::pong() {
  return SSTR("+PONG\r\n");
}

std::string Formatter::string(const std::string &str) {
  return SSTR("$" << str.length() << "\r\n" << str << "\r\n");
}

std::string Formatter::status(const std::string &str) {
  return SSTR("+" << str << "\r\n");
}

std::string Formatter::ok() {
  return "+OK\r\n";
}

std::string Formatter::null() {
  return "$-1\r\n";
}

std::string Formatter::integer(int64_t number) {
  return SSTR(":" << number << "\r\n");
}

std::string Formatter::fromStatus(const rocksdb::Status &status) {
  if(status.ok()) return Formatter::ok();
  return Formatter::err(status.ToString());
}

std::string Formatter::vector(const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return ss.str();
}

std::string Formatter::scan(const std::string &marker, const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*2\r\n";
  ss << "$" << marker.length() << "\r\n";
  ss << marker << "\r\n";

  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return ss.str();
}
