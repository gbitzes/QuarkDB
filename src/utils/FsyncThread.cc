// ----------------------------------------------------------------------
// File: FsyncThread.cc
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

#include "utils/FsyncThread.hh"
#include "utils/Macros.hh"
#include <rocksdb/db.h>

namespace quarkdb {

//------------------------------------------------------------------------------
// Construct FsyncThread object to fsync the given rocksdb every T
//------------------------------------------------------------------------------
FsyncThread::FsyncThread(rocksdb::DB *db, std::chrono::milliseconds p)
: mDB(db), mPeriod(p) {

  mThread.reset(&FsyncThread::main, this);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FsyncThread::~FsyncThread() {}

//------------------------------------------------------------------------------
// Main
//------------------------------------------------------------------------------
void FsyncThread::main(ThreadAssistant &assistant) {
  while(true) {
    assistant.wait_for(mPeriod);
    if(assistant.terminationRequested()) return;

    rocksdb::Status st = mDB->SyncWAL();
    if(!st.ok()) {
      qdb_throw("Syncing rocksdb WAL failed: " << st.ToString());
    }
  }
}

}
