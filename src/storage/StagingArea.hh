// ----------------------------------------------------------------------
// File: StagingArea.hh
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

#ifndef __QUARKDB_STAGING_AREA_H__
#define __QUARKDB_STAGING_AREA_H__

#include <mutex>
#include "KeyDescriptor.hh"
#include "../utils/SmartBuffer.hh"
#include "../StateMachine.hh"

namespace quarkdb {

#define THROW_ON_ERROR(st) { rocksdb::Status st2 = st; if(!st2.ok()) qdb_throw(st2.ToString()); }

class StagingArea {
public:
  StagingArea(StateMachine &sm) : stateMachine(sm), tx(stateMachine.startTransaction()) {
    // stateMachine.stagingMutex.lock();
  }

  ~StagingArea() {
    // stateMachine.stagingMutex.unlock();
  }

  rocksdb::Status getForUpdate(const rocksdb::Slice &slice, std::string &value) {
    return tx->GetForUpdate(rocksdb::ReadOptions(), slice, &value);
  }

  rocksdb::Status get(const rocksdb::Slice &slice, std::string &value) {
    return tx->Get(rocksdb::ReadOptions(), slice, &value);
  }

  void put(const rocksdb::Slice &slice, const rocksdb::Slice &value) {
    THROW_ON_ERROR(tx->Put(slice, value));
  }

  void del(const rocksdb::Slice &slice) {
    THROW_ON_ERROR(tx->Delete(slice));
  }

  rocksdb::Status commit(LogIndex index) {
    stateMachine.commitTransaction(tx, index);
    return rocksdb::Status::OK();
  }

private:
  StateMachine &stateMachine;
  StateMachine::TransactionPtr tx;
};

}

#endif
