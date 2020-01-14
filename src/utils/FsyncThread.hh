// ----------------------------------------------------------------------
// File: FsyncThread.hh
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

#include "utils/AssistedThread.hh"
#include <chrono>

namespace rocksdb {
  class DB;
}

namespace quarkdb {

class FsyncThread {
public:
  //----------------------------------------------------------------------------
  // Construct FsyncThread object to fsync the given rocksdb every T
  //----------------------------------------------------------------------------
  FsyncThread(rocksdb::DB *db, std::chrono::milliseconds p);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~FsyncThread();

private:
  //----------------------------------------------------------------------------
  // Main
  //----------------------------------------------------------------------------
  void main(ThreadAssistant &assistant);

  rocksdb::DB *mDB;
  std::chrono::milliseconds mPeriod;

  AssistedThread mThread;
};

}