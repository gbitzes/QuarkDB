// ----------------------------------------------------------------------
// File: Status.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QUARKDB_STATUS_H
#define QUARKDB_STATUS_H

#include "utils/Macros.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Status object for operations which may fail
//------------------------------------------------------------------------------
class Status {
public:
  //----------------------------------------------------------------------------
  // Default constructor - status is OK, no error message.
  //----------------------------------------------------------------------------
  Status() : errcode(0) {}

  //----------------------------------------------------------------------------
  // Constructor with an error
  //----------------------------------------------------------------------------
  Status(int err, std::string_view msg) : errcode(err), errorMessage(msg) {}

  //----------------------------------------------------------------------------
  // Is status ok?
  //----------------------------------------------------------------------------
  bool ok() const {
    return (errcode == 0);
  }

  //----------------------------------------------------------------------------
  // Throw FatalException if not ok
  //----------------------------------------------------------------------------
  void assertOk() const {
    if(!ok()) {
      qdb_throw("Failure (" << errcode << "): " << errorMessage);
    }
  }

  //----------------------------------------------------------------------------
  // Get errorcode
  //----------------------------------------------------------------------------
  int getErrc() const {
    return errcode;
  }

  //----------------------------------------------------------------------------
  // Get error message
  //----------------------------------------------------------------------------
  std::string getMsg() const {
    return errorMessage;
  }

private:
  int errcode;
  std::string errorMessage;
};

}

#endif
