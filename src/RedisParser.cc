// ----------------------------------------------------------------------
// File: RedisParser.cc
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

#include "RedisParser.hh"
#include "utils/Macros.hh"
#include <climits>

using namespace quarkdb;

RedisParser::RedisParser(Link *l) : reader(l) {
}

int RedisParser::purge() {
  request_size = 0;
  current_element = 0;
  element_size = -1;

  current_integer.clear();
  current_request.clear();

  std::string buff;
  buff.resize(1024);
  while(true) {
    int rlen = reader.consume(1023, buff);
    if(rlen <= 0) return rlen;
  }
}

int RedisParser::fetch(RedisRequest &req, bool authenticated) {
  if(request_size == 0) {
    req.clear();

    // new request to process from scratch
    int retcode = readInteger('*', request_size);
    if(retcode <= 0) return retcode;

    element_size = -1;
    current_element = 0;

    if(!authenticated && request_size >= 5) {
      qdb_warn("Unauthenticated client attempted to send request with " << request_size << " elements - shutting the connection down");
      return -2;
    }

    req.resize(request_size);
  }

  for( ; current_element < request_size; current_element++) {
    int rc = readElement(req.getPinnedBuffer(current_element), authenticated);
    if(rc <= 0) return rc;
    element_size = -1;
  }

  request_size = 0;
  req.parseCommand();

  if(encounteredZeroSize) {
    qdb_warn("Encountered request with zero-sized string - shutting the connection down: " << req.toPrintableString());
    return -2;
  }

  return 1;
}

int RedisParser::readInteger(char prefix, int &retval) {
  std::string prev;

  while(prev[0] != '\n') {
    int rlen = reader.consume(1, prev);
    if(rlen <= 0) return rlen;

    current_integer.append(prev);
  }

  if(current_integer[0] != prefix) {
    qdb_warn("Redis protocol error, expected an integer with preceeding " << quotes(prefix) << ", received " << quotes(current_integer[0]) << " instead (byte in decimal: " << int(current_integer[0]) << ")");
    return -1;
  }

  if(current_integer[current_integer.size()-2] != '\r') {
    qdb_warn("Redis protocol error, received \\n without preceeding \\r");
    return -1;
  }

  current_integer.erase(current_integer.size()-2, 2);

  char *endptr;
  long num = strtol(current_integer.c_str()+1, &endptr, 10);
  if(*endptr != '\0' || num == LONG_MIN || num == LONG_MAX) {
    qdb_warn("Redis protocol error, received an invalid integer");
    return -1;
  }

  current_integer = "";
  retval = num;
  return 1; // success
}

int RedisParser::readElement(PinnedBuffer &str, bool authenticated) {
  if(element_size == -1) {
    int retcode = readInteger('$', element_size);
    if(retcode <= 0) return retcode;
    if(element_size == 0) encounteredZeroSize = true;
  }

  if(!authenticated && element_size >= 1048576) {
    qdb_warn("Unauthenticated client attempted to send request containing element with " << element_size << " bytes - shutting the connection down");
    return -2;
  }

  return readString(element_size, str);
}

int RedisParser::readString(int nbytes, PinnedBuffer &str) {
  int rlen = reader.consume(nbytes+2, str);
  if(rlen <= 0) return rlen;

  if(str[str.size()-2] != '\r') {
    qdb_warn("Redis protocol error, expected \\r, received " << str[str.size()-2]);
    return -1;
  }

  if(str[str.size()-1] != '\n') {
    qdb_warn("Redis protocol error, expected \\n, received " << str[str.size()-1]);
    return -1;
  }

  str.remove_suffix(2);
  return rlen;
}
