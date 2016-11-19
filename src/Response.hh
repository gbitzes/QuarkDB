//-----------------------------------------------------------------------
// File: Response.hh
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

#ifndef __QUARKDB_RESPONSE_H__
#define __QUARKDB_RESPONSE_H__

#include "Common.hh"
#include "Link.hh"
#include <rocksdb/status.h>
#include <string>

namespace quarkdb {

class Response {
public:
  static LinkStatus err(Link *link, const std::string &msg);
  static LinkStatus errArgs(Link *link, const std::string &cmd);
  static LinkStatus pong(Link *link);
  static LinkStatus string(Link *link, const std::string &str);
  static LinkStatus fromStatus(Link *link, const rocksdb::Status &status);
  static LinkStatus status(Link *link, const std::string &str);
  static LinkStatus ok(Link *link);
  static LinkStatus null(Link *link);
  static LinkStatus integer(Link *link, int64_t number);
  static LinkStatus vector(Link *link, const std::vector<std::string> &vec);
  static LinkStatus scan(Link *link, const std::string &marker, const std::vector<std::string> &vec);
};

}

#endif
