// ----------------------------------------------------------------------
// File: OptionUtils.hh
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

#ifndef __QUARKDB_OPTION_UTILS_H__
#define __QUARKDB_OPTION_UTILS_H__

#include "optionparser.h"

namespace Opt {

inline option::ArgStatus nonempty(const option::Option& option, bool msg) {
  if (option.arg != 0 && option.arg[0] != 0)
    return option::ARG_OK;
  if (msg) std::cout << "Option '" << option.name << "' requires a non-empty argument" << std::endl;
    return option::ARG_ILLEGAL;
}

inline option::ArgStatus numeric(const option::Option& option, bool msg) {
  char* endptr = 0;
  if (option.arg != 0 && strtol(option.arg, &endptr, 10)){};
  if (endptr != option.arg && *endptr == 0)
    return option::ARG_OK;

  if (msg) std::cout << "Option '" << option.name << "' requires a numeric argument" << std::endl;
    return option::ARG_ILLEGAL;
}

}

#endif
