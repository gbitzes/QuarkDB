// ----------------------------------------------------------------------
// File: VersionedHashRevisionTracker.cc
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

#include "VersionedHashRevisionTracker.hh"
#include "utils/Macros.hh"
#include "Formatter.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Indicate which revision we're referring to. When called multiple times
// for the same object, the given value MUST be the same.
//------------------------------------------------------------------------------
void VersionedHashRevision::setRevisionNumber(uint64_t rev) {
  if(currentRevision != 0) {
    qdb_assert(currentRevision == rev);
  }
  else {
    currentRevision = rev;
  }
}

//------------------------------------------------------------------------------
// Add to update batch - empty value indicates deletion
//------------------------------------------------------------------------------
void VersionedHashRevision::addUpdate(std::string_view field, std::string_view value) {
  updateBatch.emplace_back(field, value);
}

//------------------------------------------------------------------------------
// Serialize contents
//------------------------------------------------------------------------------
std::string VersionedHashRevision::serialize() const {
  return Formatter::vhashRevision(currentRevision, updateBatch).val;
}

//------------------------------------------------------------------------------
// Get revision for a specific key
//------------------------------------------------------------------------------
VersionedHashRevision& VersionedHashRevisionTracker::forKey(std::string_view key) {
  return contents[std::string(key)];
}

//------------------------------------------------------------------------------
// Iterate through contents
//------------------------------------------------------------------------------
std::map<std::string, VersionedHashRevision>::iterator VersionedHashRevisionTracker::begin() {
  return contents.begin();
}

std::map<std::string, VersionedHashRevision>::iterator VersionedHashRevisionTracker::end() {
  return contents.end();
}


}
