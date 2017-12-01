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

RedisEncodedResponse Formatter::moved(int64_t shardId, const RaftServer &location) {
  return RedisEncodedResponse(SSTR("-MOVED " << shardId << " " << location.toString() << "\r\n"));
}

RedisEncodedResponse Formatter::err(const std::string &err) {
  return RedisEncodedResponse(SSTR("-ERR " << err << "\r\n"));
}

RedisEncodedResponse Formatter::errArgs(const std::string &cmd) {
  return RedisEncodedResponse(SSTR("-ERR wrong number of arguments for '" << cmd << "' command\r\n"));
}

RedisEncodedResponse Formatter::pong() {
  return RedisEncodedResponse(SSTR("+PONG\r\n"));
}

RedisEncodedResponse Formatter::string(const std::string &str) {
  return RedisEncodedResponse(SSTR("$" << str.length() << "\r\n" << str << "\r\n"));
}

RedisEncodedResponse Formatter::status(const std::string &str) {
  return RedisEncodedResponse(SSTR("+" << str << "\r\n"));
}

RedisEncodedResponse Formatter::ok() {
  return RedisEncodedResponse("+OK\r\n");
}

RedisEncodedResponse Formatter::null() {
  return RedisEncodedResponse("$-1\r\n");
}

RedisEncodedResponse Formatter::integer(int64_t number) {
  return RedisEncodedResponse(SSTR(":" << number << "\r\n"));
}

RedisEncodedResponse Formatter::fromStatus(const rocksdb::Status &status) {
  if(status.ok()) return Formatter::ok();
  return Formatter::err(status.ToString());
}

RedisEncodedResponse Formatter::vector(const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return RedisEncodedResponse(ss.str());
}

RedisEncodedResponse Formatter::scan(const std::string &marker, const std::vector<std::string> &vec) {
  std::stringstream ss;
  ss << "*2\r\n";
  ss << "$" << marker.length() << "\r\n";
  ss << marker << "\r\n";

  ss << "*" << vec.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = vec.begin(); it != vec.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return RedisEncodedResponse(ss.str());
}
