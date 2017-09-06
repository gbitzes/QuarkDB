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
  StagingArea(StateMachine &sm) : stateMachine(sm), bulkLoad(stateMachine.inBulkLoad()) {

    if(!bulkLoad) {
      tx = stateMachine.startTransaction();
    }
  }

  ~StagingArea() { }

  rocksdb::Status getForUpdate(const rocksdb::Slice &slice, std::string &value) {
    if(bulkLoad) {
      return rocksdb::Status::NotFound();
    }

    return tx->GetForUpdate(rocksdb::ReadOptions(), slice, &value);
  }

  rocksdb::Status get(const rocksdb::Slice &slice, std::string &value) {
    if(bulkLoad) {
      return rocksdb::Status::NotFound();
    }

    return tx->Get(rocksdb::ReadOptions(), slice, &value);
  }

  void put(const rocksdb::Slice &slice, const rocksdb::Slice &value) {
    if(bulkLoad) {
      if(slice[0] == char(InternalKeyType::kDescriptor)) {
        // Ignore key descriptors, we'll build them all at the end
        return;
      }

      // rocksdb transactions have to build an internal index to implement
      // repeatable reads on the same tx. In bulkload mode we don't allow reads,
      // so let's use the much faster write batch.
      writeBatch.Put(slice, value);
      return;
    }

    THROW_ON_ERROR(tx->Put(slice, value));
  }

  void del(const rocksdb::Slice &slice) {
    if(bulkLoad) qdb_throw("no deletions allowed during bulk load");
    THROW_ON_ERROR(tx->Delete(slice));
  }

  rocksdb::Status commit(LogIndex index) {
    if(bulkLoad) {
      qdb_assert(index == 0);
      stateMachine.commitBatch(writeBatch);
      return rocksdb::Status::OK();
    }

    stateMachine.commitTransaction(tx, index);
    return rocksdb::Status::OK();
  }

private:
  StateMachine &stateMachine;
  bool bulkLoad = false;
  StateMachine::TransactionPtr tx;
  rocksdb::WriteBatch writeBatch;
};

}

#endif
