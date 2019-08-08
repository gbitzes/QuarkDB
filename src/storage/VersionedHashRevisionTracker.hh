// ----------------------------------------------------------------------
// File: VersionedHashRevisionTracker.hh
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

#ifndef QUARKDB_VERSIONED_HASH_REVISION_TRACKER_HH
#define QUARKDB_VERSIONED_HASH_REVISION_TRACKER_HH

#include <vector>
#include <string_view>
#include <map>

namespace quarkdb {

//------------------------------------------------------------------------------
// Describes updates during a single revision for a specific versioned hash.
//------------------------------------------------------------------------------
class VersionedHashRevision {
public:
  //----------------------------------------------------------------------------
  // Indicate which revision we're referring to. When called multiple times
  // for the same object, the given value MUST be the same.
  //----------------------------------------------------------------------------
  void setRevisionNumber(uint64_t rev);

  //----------------------------------------------------------------------------
  // Add to update batch - empty value indicates deletion
  //----------------------------------------------------------------------------
  void addUpdate(std::string_view field, std::string_view value);

private:
  uint64_t currentRevision = 0;
  std::vector<std::pair<std::string_view, std::string_view>> updateBatch;
};

//------------------------------------------------------------------------------
// Tracks all revisions during a single transaction, which could affect
// multiple keys.
//------------------------------------------------------------------------------
class VersionedHashRevisionTracker {
public:
  //----------------------------------------------------------------------------
  // Get revision for a specific key
  //----------------------------------------------------------------------------
  VersionedHashRevision& forKey(std::string_view key);

  //----------------------------------------------------------------------------
  // Iterate through contents
  //----------------------------------------------------------------------------
  std::map<std::string, VersionedHashRevision>::iterator begin();
  std::map<std::string, VersionedHashRevision>::iterator end();

private:
  std::map<std::string, VersionedHashRevision> contents;
};

}

#endif
