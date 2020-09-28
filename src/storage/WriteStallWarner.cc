// ----------------------------------------------------------------------
// File: WriteStallWarner.cc
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

#include "WriteStallWarner.hh"
#include "utils/Macros.hh"
#include <sstream>

namespace quarkdb {

std::string stallConditionToString(const rocksdb::WriteStallCondition& cond) {
  if(cond == rocksdb::WriteStallCondition::kNormal) {
    return "normal";
  }

  if(cond == rocksdb::WriteStallCondition::kDelayed) {
    return "delayed";
  }

  if(cond == rocksdb::WriteStallCondition::kStopped) {
    return "stopped";
  }

  return "???";
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
WriteStallWarner::WriteStallWarner(const std::string &name)
: mName(name) {}

//------------------------------------------------------------------------------
// Change in stall condition
//------------------------------------------------------------------------------
void WriteStallWarner::OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) {
  std::ostringstream ss;

  ss << "Change in write-stall condition (" << mName << "): " << stallConditionToString(info.condition.prev) <<
    " => " << stallConditionToString(info.condition.cur);

  if(info.condition.cur != rocksdb::WriteStallCondition::kNormal) {
    qdb_warn(ss.str());
  }
  else {
    qdb_info(ss.str());
  }
}

}

