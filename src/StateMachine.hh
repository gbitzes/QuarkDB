// ----------------------------------------------------------------------
// File: StateMachine.hh
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

#ifndef __QUARKDB_ROCKSDB_H__
#define __QUARKDB_ROCKSDB_H__

#include "Common.hh"
#include "Utils.hh"
#include <rocksdb/db.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/transaction.h>

namespace quarkdb {

class StateMachine {
public:
  StateMachine(const std::string &filename);
  virtual ~StateMachine();
  DISALLOW_COPY_AND_ASSIGN(StateMachine);
  void reset();

  using VecIterator = std::vector<std::string>::const_iterator;
  using IteratorPtr = std::shared_ptr<rocksdb::Iterator>;
  using TransactionPtr = std::shared_ptr<rocksdb::Transaction>;

  //----------------------------------------------------------------------------
  // Main API
  //----------------------------------------------------------------------------

  rocksdb::Status hget(const std::string &key, const std::string &field, std::string &value);
  rocksdb::Status hexists(const std::string &key, const std::string &field);
  rocksdb::Status hkeys(const std::string &key, std::vector<std::string> &keys);
  rocksdb::Status hgetall(const std::string &key, std::vector<std::string> &res);
  rocksdb::Status hset(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index = 0);
  rocksdb::Status hsetnx(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index = 0);
  rocksdb::Status hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result, LogIndex index = 0);
  rocksdb::Status hincrbyfloat(const std::string &key, const std::string &field, const std::string &incrby, double &result, LogIndex index = 0);
  rocksdb::Status hdel(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index = 0);
  rocksdb::Status hlen(const std::string &key, size_t &len);
  rocksdb::Status hscan(const std::string &key, const std::string &cursor, size_t count, std::string &newcursor, std::vector<std::string> &results);
  rocksdb::Status hvals(const std::string &key, std::vector<std::string> &vals);
  rocksdb::Status sadd(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &added, LogIndex index = 0);
  rocksdb::Status sismember(const std::string &key, const std::string &element);
  rocksdb::Status srem(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index = 0);
  rocksdb::Status smembers(const std::string &key, std::vector<std::string> &members);
  rocksdb::Status scard(const std::string &key, size_t &count);
  rocksdb::Status set(const std::string& key, const std::string& value, LogIndex index = 0);
  rocksdb::Status get(const std::string &key, std::string &value);
  rocksdb::Status del(const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index = 0);
  rocksdb::Status exists(const std::string &key);
  rocksdb::Status keys(const std::string &pattern, std::vector<std::string> &result);
  rocksdb::Status flushall(LogIndex index = 0);

  rocksdb::Status noop(LogIndex index);
  LogIndex getLastApplied();

  //----------------------------------------------------------------------------
  // Checkpoint for online backups
  //----------------------------------------------------------------------------
  rocksdb::Status checkpoint(const std::string &path);

  //----------------------------------------------------------------------------
  // Get statistics
  //----------------------------------------------------------------------------
  std::string statistics();

  enum class KeyType : char {
    kString = 'a',
    kHash = 'b',
    kSet = 'c'
  };

  enum class InternalKeyType : char {
    kInternal = '_',
    kDescriptor = 'd'
  };

private:
  class Snapshot {
  public:
    Snapshot(rocksdb::DB *db);
    ~Snapshot();
    rocksdb::ReadOptions& opts();
  private:
    rocksdb::DB *db;
    const rocksdb::Snapshot *snapshot;
    rocksdb::ReadOptions options;
    DISALLOW_COPY_AND_ASSIGN(Snapshot);
  };

  TransactionPtr startTransaction();
  void commitTransaction(TransactionPtr &tx, LogIndex index);
  rocksdb::Status finalize(TransactionPtr &tx, LogIndex index);
  rocksdb::Status malformedRequest(TransactionPtr &tx, LogIndex index, std::string message);
  bool assertKeyType(Snapshot &snapshot, const std::string &key, KeyType keytype);

  struct KeyDescriptor {
    bool exists;
    std::string dkey;
    KeyType keytype;
    int64_t size;

    std::string serialize() const;
    static KeyDescriptor construct(const rocksdb::Status &st, const std::string &str, std::string &&tkey);
  };

  class WriteOperation {
  public:
    WriteOperation(TransactionPtr &tx, const std::string &key, const KeyType &type);
    ~WriteOperation();

    bool valid();
    bool keyExists();
    bool getField(const std::string &field, std::string &out);
    int64_t keySize();

    void assertWritable();

    void write(const std::string &value);
    void writeField(const std::string &field, const std::string &value);
    bool fieldExists(const std::string &field);
    bool deleteField(const std::string &field);

    void finalize(int64_t newsize);
  private:
    TransactionPtr &tx;
    const std::string &redisKey;

    KeyType expectedType;
    KeyDescriptor keyinfo;

    bool redisKeyExists;
    bool isValid = false;
    bool finalized = false;
  };
  friend class WriteOperation;

  KeyDescriptor getKeyDescriptor(const std::string &redisKey);
  KeyDescriptor getKeyDescriptor(Snapshot &snapshot, const std::string &redisKey);
  KeyDescriptor lockKeyDescriptor(TransactionPtr &tx, const std::string &redisKey);

  rocksdb::Status wrongKeyType(TransactionPtr &tx, LogIndex index);

  void retrieveLastApplied();
  void ensureCompatibleFormat(bool justCreated);
  void remove_all_with_prefix(const std::string &prefix, int64_t &removed, TransactionPtr &tx);

  LogIndex lastApplied;
  rocksdb::TransactionDB* transactionDB = nullptr;
  rocksdb::DB* db = nullptr;
  const std::string filename;
};


}

#endif
