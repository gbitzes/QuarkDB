// ----------------------------------------------------------------------
// File: RedisParser.hh
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

#ifndef __QUARKDB_REDIS_PARSER_H__
#define __QUARKDB_REDIS_PARSER_H__

#include "BufferedReader.hh"
#include "Common.hh"
#include "RedisRequest.hh"

namespace quarkdb {

class RedisParser {
public:
  RedisParser(Link *link);

  //----------------------------------------------------------------------------
  // Resumable function to fetch a request from the link.
  // returns 1 if there's a request to process, and sets req accordingly
  // returns 0 on slow link, ie if there's not enough data on the link to
  //           construct a redis request from
  // returns negative on error
  //----------------------------------------------------------------------------

  LinkStatus fetch(RedisRequest &req);

  //----------------------------------------------------------------------------
  // Purge any and all incoming data.
  // Useful for connections which are in a special state, such as "MONITOR".
  //
  // After calling purge() even once, any calls to fetch() have completely
  // undefined behaviour - we simply can't salvage the connection for reading.
  //
  // Why even have such a function ? To avoid the poller (in this case meaning
  // Poller.hh or xrootd scheduler) from waking up constantly and eating up
  // CPU time if the user accidentally sends data from redis-cli.
  // This will consume any input and calm down the poller.
  //----------------------------------------------------------------------------
  LinkStatus purge();

private:
  BufferedReader reader;

  //----------------------------------------------------------------------------
  // A redis request is composed of multiple elements.
  // Here we keep track the size of the current element as well as how many
  // elements we've read so far.
  //----------------------------------------------------------------------------

  int request_size = 0;
  int current_element = 0;
  int element_size = 0;

  //----------------------------------------------------------------------------
  // Buffers to hold temporary contents.
  //----------------------------------------------------------------------------

  std::string current_integer;
  RedisRequest current_request;

  //----------------------------------------------------------------------------
  // Helper functions to do parsing
  //----------------------------------------------------------------------------

  int readInteger(char prefix);
  int readElement(std::string &str);
  int readString(int nbytes, std::string &str);

};

}

#endif
