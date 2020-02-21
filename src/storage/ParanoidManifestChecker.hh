// ----------------------------------------------------------------------
// File: ParanoidManifestChecker.hh
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

#ifndef QUARKDB_PARANOID_MANIFEST_CHECKER_HH
#define QUARKDB_PARANOID_MANIFEST_CHECKER_HH

#include "utils/AssistedThread.hh"
#include <rocksdb/db.h>
#include <string_view>
#include "Status.hh"
#include "utils/Synchronized.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// We've observed in the past RocksDB corrupting its MANIFEST file, while new
// SST files were being written.
//
// This is an attempt at detecting this problem early, but we're not sure if
// it works, or how useful it might be.
//------------------------------------------------------------------------------
class ParanoidManifestChecker {
public:
  //----------------------------------------------------------------------------
  // Constructor receiving the rocksdb path
  //----------------------------------------------------------------------------
  ParanoidManifestChecker(std::string_view path);

  //----------------------------------------------------------------------------
  // Main thread checking the status on regular intervals
  //----------------------------------------------------------------------------
  void main(ThreadAssistant &assistant);

  //----------------------------------------------------------------------------
  // Check the given DB path
  //----------------------------------------------------------------------------
  static Status checkDB(std::string_view path);

  //----------------------------------------------------------------------------
  // Get last status
  //----------------------------------------------------------------------------
  Status getLastStatus() const;

private:
  AssistedThread mThread;
  std::string mPath;
  Synchronized<Status> mLastStatus;
};

}

#endif
