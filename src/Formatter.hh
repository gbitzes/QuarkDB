//-----------------------------------------------------------------------
// File: Formatter.hh
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

#ifndef __QUARKDB_FORMATTER_H__
#define __QUARKDB_FORMATTER_H__

#include <string>
#include <rocksdb/status.h>
#include "Utils.hh"

namespace quarkdb {

class Formatter {
public:
  static std::string err(const std::string &msg);
  static std::string errArgs(const std::string &cmd);
  static std::string pong();
  static std::string string(const std::string &str);
  static std::string fromStatus(const rocksdb::Status &status);
  static std::string status(const std::string &str);
  static std::string ok();
  static std::string null();
  static std::string integer(int64_t number);
  static std::string vector(const std::vector<std::string> &vec);
  static std::string scan(const std::string &marker, const std::vector<std::string> &vec);
};

}

#endif
