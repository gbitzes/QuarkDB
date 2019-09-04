// ----------------------------------------------------------------------
// File: CommandParsing.hh
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

#ifndef QUARKDB_COMMAND_PARSING_HH
#define QUARKDB_COMMAND_PARSING_HH

#include "utils/StringUtils.hh"
#include "RedisRequest.hh"
#include "utils/ParseUtils.hh"

#include <string>
#include <dirent.h>

namespace quarkdb {

struct ScanCommandArguments {
  std::string cursor;
  int64_t count = 100;
  std::string match;
  std::string error;
};

inline ScanCommandArguments parseScanCommand(const RedisRequest::const_iterator &begin, const RedisRequest::const_iterator &end, bool supportMatch) {
  qdb_assert(begin != end);

  ScanCommandArguments args;

  // Set cursor
  if(*begin == "0") {
    args.cursor = "";
  }
  else if(StringUtils::startsWith(*begin, "next:")) {
    args.cursor = std::string(begin->data() + 5, begin->size() - 5);
  }
  else {
    args.error = "invalid cursor";
    return args;
  }

  // Cursor is ok - look for MATCH / COUNT
  RedisRequest::const_iterator pos = begin+1;
  while(pos != end) {
    if(pos+1 == end) {
      // Odd number of arguments, bail out
      args.error = "syntax error";
      return args;
    }

    if(caseInsensitiveEquals(*pos, "count")) {
      RedisRequest::const_iterator count = pos+1;
      // Parse integer, only allowing non-zero values.
      if(StringUtils::startsWith(*count, "-") || *count == "0") {
        args.error = "syntax error";
        return args;
      }

      if(!ParseUtils::parseInt64(*count, args.count)) {
        args.error = "value is not an integer or out of range";
        return args;
      }
    }
    else if(caseInsensitiveEquals(*pos, "match") && supportMatch) {
      args.match = *(pos+1);
    }
    else {
      // Unknown argument
      args.error = "syntax error";
      return args;
    }

    pos += 2;
  }

  return args;
}

}

#endif
