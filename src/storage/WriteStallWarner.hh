// ----------------------------------------------------------------------
// File: WriteStallWarner.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#ifndef QUARKDB_WRITE_STALL_WARNER_HH
#define QUARKDB_WRITE_STALL_WARNER_HH

#include <vector>
#include <string_view>
#include <map>
#include <rocksdb/listener.h>

namespace quarkdb {

//------------------------------------------------------------------------------
// Describes updates during a single revision for a specific versioned hash.
//------------------------------------------------------------------------------
class WriteStallWarner : public rocksdb::EventListener {
public:

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  WriteStallWarner(const std::string &name);

  //----------------------------------------------------------------------------
  // Change in stall condition
  //----------------------------------------------------------------------------
  virtual void OnStallConditionsChanged(const rocksdb::WriteStallInfo& /*info*/) override;

private:
  std::string mName;
};

}

#endif
