// ----------------------------------------------------------------------
// File: Stacktrace.hh
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

#ifndef __QUARKDB_UTILS_STACKTRACE_H__
#define __QUARKDB_UTILS_STACKTRACE_H__

#define BACKWARD_HAS_DW 1

#include <backward.hpp>

namespace quarkdb {

inline std::string getStacktrace() {
  std::ostringstream ss;

  backward::StackTrace st; st.load_here(32);
  backward::Printer p;
	p.object = true;
	p.color_mode = backward::ColorMode::always;
	p.address = true;
	p.print(st, ss);

	return ss.str();
}

}

#endif
