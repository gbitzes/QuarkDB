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

#ifndef QUARKDB_STATE_MACHINE_H
#define QUARKDB_STATE_MACHINE_H

#include "Timekeeper.hh"
#include "Common.hh"
#include "utils/Macros.hh"
#include "utils/RequestCounter.hh"
#include "storage/KeyDescriptor.hh"
#include "storage/KeyLocators.hh"
#include "storage/ConsistencyScanner.hh"
#include "storage/KeyConstants.hh"
#include "storage/LeaseInfo.hh"
#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/utilities/debug.h>
#include <condition_variable>

namespace quarkdb {

enum class LeaseAcquisitionStatus {
  kKeyTypeMismatch,
  kAcquired,
  kRenewed,
  kFailedDueToOtherOwner
};

class StagingArea;

class StateMachine {
public:
  StateMachine(std::string_view filename, bool write_ahead_log = true, bool bulkLoad = false);
  virtual ~StateMachine();
  DISALLOW_COPY_AND_ASSIGN(StateMachine);
  void reset();

  using VecIterator = std::vector<std::string>::const_iterator;
  using IteratorPtr = std::unique_ptr<rocksdb::Iterator>;

  //----------------------------------------------------------------------------
  // API for transactional writes - in this case, lastApplied increments once
  // per batch, not per individual operation.
  //----------------------------------------------------------------------------
  rocksdb::Status set(StagingArea &stagingArea, std::string_view key, std::string_view value);
  rocksdb::Status del(StagingArea &stagingArea, const VecIterator &start, const VecIterator &end, int64_t &removed);
  rocksdb::Status flushall(StagingArea &stagingArea);

  rocksdb::Status hset(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view value, bool &fieldcreated);
  rocksdb::Status hmset(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end);
  rocksdb::Status hsetnx(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view value, bool &fieldcreated);
  rocksdb::Status hincrby(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view incrby, int64_t &result);
  rocksdb::Status hincrbyfloat(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view incrby, double &result);
  rocksdb::Status hdel(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end, int64_t &removed);
  rocksdb::Status hclone(StagingArea &stagingArea, std::string_view source, std::string_view target);

  rocksdb::Status lhset(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view hint, std::string_view value, bool &fieldcreated);
  rocksdb::Status lhdel(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end, int64_t &removed);
  rocksdb::Status lhmset(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end);

  rocksdb::Status sadd(StagingArea &stagingArea, std::string_view key,  const VecIterator &start, const VecIterator &end, int64_t &added);
  rocksdb::Status srem(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end, int64_t &removed);
  rocksdb::Status smove(StagingArea &stagingArea, std::string_view source, std::string_view destination, std::string_view element, int64_t &outcome);

  rocksdb::Status dequePushFront(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end, int64_t &length);
  rocksdb::Status dequePushBack(StagingArea &stagingArea, std::string_view key, const VecIterator &start, const VecIterator &end, int64_t &length);
  rocksdb::Status dequePopFront(StagingArea &stagingArea, std::string_view key, std::string &item);
  rocksdb::Status dequePopBack(StagingArea &stagingArea, std::string_view key, std::string &item);
  rocksdb::Status dequeTrimFront(StagingArea &stagingArea, std::string_view key, std::string_view maxToKeep, int64_t &itemsRemoved);

  void advanceClock(StagingArea &stagingArea, ClockValue newValue);
  LeaseAcquisitionStatus lease_acquire(StagingArea &stagingArea, const std::string &key, const std::string &value, ClockValue clockUpdate, uint64_t duration, LeaseInfo &info);
  rocksdb::Status lease_release(StagingArea &stagingArea, const std::string &key, ClockValue clockValue);
  rocksdb::Status lease_get(StagingArea &stagingArea, const std::string &key, ClockValue clockUpdate, LeaseInfo &info);

  //----------------------------------------------------------------------------
  // API for transactional reads. Can be part of a mixed read-write transaction.
  //----------------------------------------------------------------------------
  rocksdb::Status get(StagingArea &stagingArea, std::string_view key, std::string &value);
  rocksdb::Status exists(StagingArea &stagingArea, const VecIterator &start, const VecIterator &end, int64_t &count);
  rocksdb::Status keys(StagingArea &stagingArea, const std::string &pattern, std::vector<std::string> &result);
  rocksdb::Status scan(StagingArea &stagingArea, const std::string &cursor, const std::string &pattern, size_t count, std::string &newcursor, std::vector<std::string> &results);
  rocksdb::Status hget(StagingArea &stagingArea, const std::string &key, const std::string &field, std::string &value);
  rocksdb::Status hexists(StagingArea &stagingArea, const std::string &key, const std::string &field);
  rocksdb::Status hkeys(StagingArea &stagingArea, const std::string &key, std::vector<std::string> &keys);
  rocksdb::Status hgetall(StagingArea &stagingArea, const std::string &key, std::vector<std::string> &res);
  rocksdb::Status hlen(StagingArea &stagingArea, const std::string &key, size_t &len);
  rocksdb::Status hvals(StagingArea &stagingArea, const std::string &key, std::vector<std::string> &vals);
  rocksdb::Status hscan(StagingArea &stagingArea, const std::string &key, const std::string &cursor, size_t count, std::string &newcursor, std::vector<std::string> &results);
  rocksdb::Status sismember(StagingArea &stagingArea, const std::string &key, const std::string &element);
  rocksdb::Status smembers(StagingArea &stagingArea, const std::string &key, std::vector<std::string> &members);
  rocksdb::Status scard(StagingArea &stagingArea, const std::string &key, size_t &count);
  rocksdb::Status sscan(StagingArea &stagingArea, const std::string &key, const std::string &cursor, size_t count, std::string &newCursor, std::vector<std::string> &res);
  rocksdb::Status dequeLen(StagingArea &stagingArea, const std::string &key, size_t &len);
  rocksdb::Status lhget(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &hint, std::string &value);
  rocksdb::Status lhlen(StagingArea &stagingArea, const std::string &key, size_t &len);
  rocksdb::Status lhscan(StagingArea &stagingArae, std::string_view key, std::string_view cursor, size_t count, std::string &newcursor, std::vector<std::string> &results);
  rocksdb::Status rawScan(StagingArea &stagingArea, const std::string &key, size_t count, std::vector<std::string> &elements);
  rocksdb::Status dequeScanBack(StagingArea &stagingArea, std::string_view key, std::string_view cursor, size_t count, std::string &newCursor, std::vector<std::string> &results);
  void getClock(StagingArea &stagingArea, ClockValue &value);
  void getType(StagingArea &stagingArea, std::string_view key, std::string& keyType);

  //----------------------------------------------------------------------------
  // Simple API
  //----------------------------------------------------------------------------
  rocksdb::Status hget(const std::string &key, const std::string &field, std::string &value);
  rocksdb::Status hexists(const std::string &key, const std::string &field);
  rocksdb::Status hkeys(const std::string &key, std::vector<std::string> &keys);
  rocksdb::Status hgetall(const std::string &key, std::vector<std::string> &res);
  rocksdb::Status hset(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index = 0);
  rocksdb::Status hmset(std::string_view key, const VecIterator &start, const VecIterator &end, LogIndex index = 0);
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
  rocksdb::Status sscan(const std::string &key, const std::string &cursor, size_t count, std::string &newCursor, std::vector<std::string> &res);
  rocksdb::Status set(const std::string& key, const std::string& value, LogIndex index = 0);
  rocksdb::Status get(std::string_view key, std::string &value);
  rocksdb::Status del(const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index = 0);
  rocksdb::Status exists(const VecIterator &start, const VecIterator &end, int64_t &count);
  rocksdb::Status keys(const std::string &pattern, std::vector<std::string> &result);
  rocksdb::Status scan(const std::string &cursor, const std::string &pattern, size_t count, std::string &newcursor, std::vector<std::string> &results);
  rocksdb::Status flushall(LogIndex index = 0);
  rocksdb::Status dequePushFront(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index = 0);
  rocksdb::Status dequePushBack(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index = 0);
  rocksdb::Status dequePopFront(const std::string &key, std::string &item, LogIndex index = 0);
  rocksdb::Status dequePopBack(const std::string &key, std::string &item, LogIndex index = 0);
  rocksdb::Status dequeTrimFront(const std::string &key, const std::string &maxToKeep, int64_t &itemsRemoved, LogIndex index = 0);
  rocksdb::Status dequeLen(const std::string &key, size_t &len);
  rocksdb::Status lhset(const std::string &key, const std::string &field, const std::string &hint, const std::string &value, bool &fieldcreated, LogIndex index = 0);
  rocksdb::Status lhlen(const std::string &key, size_t &len);
  rocksdb::Status lhget(const std::string &key, const std::string &field, const std::string &hint, std::string &value);
  void advanceClock(ClockValue newValue, LogIndex index = 0);
  void getClock(ClockValue &value);
  rocksdb::Status rawGetAllVersions(const std::string &key, std::vector<rocksdb::KeyVersion> &versions);
  LeaseAcquisitionStatus lease_acquire(const std::string &key, const std::string &value, ClockValue clockUpdate, uint64_t duration, LeaseInfo &info, LogIndex index = 0);
  rocksdb::Status lease_release(const std::string &key, ClockValue clockUpdate, LogIndex index = 0);
  rocksdb::Status lease_get(const std::string &key, ClockValue clockUpdate, LeaseInfo &info, LogIndex index = 0);

  //----------------------------------------------------------------------------
  // Internal configuration, not exposed to users through 'KEYS' and friends.
  // Has completely its own key namespace and does not interact in any way
  // with the other redis commands.
  //----------------------------------------------------------------------------
  rocksdb::Status configGet(const std::string &key, std::string &value);
  rocksdb::Status configGet(StagingArea &stagingArea, const std::string &key, std::string &value);

  rocksdb::Status configSet(const std::string &key, const std::string &value, LogIndex index = 0);
  rocksdb::Status configSet(StagingArea &stagingArea, const std::string &key, const std::string &value);

  rocksdb::Status configGetall(std::vector<std::string> &res);
  rocksdb::Status configGetall(StagingArea &stagingArea, std::vector<std::string> &res);

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

  //----------------------------------------------------------------------------
  // Get level stats
  //----------------------------------------------------------------------------
  std::string levelStats();

  //----------------------------------------------------------------------------
  // Get compression stats
  //----------------------------------------------------------------------------
  std::vector<std::string> compressionStats();

  bool inBulkLoad() const {
    return bulkLoad;
  }

  rocksdb::Status manualCompaction();
  void finalizeBulkload();
  IteratorPtr getRawIterator();
  void commitBatch(rocksdb::WriteBatch &batch);
  bool waitUntilTargetLastApplied(LogIndex targetLastApplied, std::chrono::milliseconds duration);
  rocksdb::Status verifyChecksum();
  RequestCounter& getRequestCounter() { return requestCounter; }

  ClockValue getDynamicClock();
  void hardSynchronizeDynamicClock();

private:
  ClockValue maybeAdvanceClock(StagingArea &stagingArea, ClockValue newValue);
  friend class StagingArea;

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

  void commitTransaction(rocksdb::WriteBatchWithIndex &wb, LogIndex index);
  bool assertKeyType(StagingArea &stagingArea, std::string_view key, KeyType keytype);
  rocksdb::Status dequePop(StagingArea &stagingArea, Direction direction, std::string_view key, std::string &item);
  rocksdb::Status dequePush(StagingArea &stagingArea, Direction direction, std::string_view key, const VecIterator &start, const VecIterator &end, int64_t &length);

  class WriteOperation {
  public:
    WriteOperation(StagingArea &stagingArea, std::string_view key, const KeyType &type);
    ~WriteOperation();

    bool valid();
    bool keyExists();
    bool getField(std::string_view field, std::string &out);
    bool getLocalityIndex(std::string_view field, std::string &out);
    void cancel();

    int64_t keySize();

    void assertWritable();

    void write(std::string_view value);
    void writeField(std::string_view field, std::string_view value);
    void writeLocalityField(std::string_view hint, std::string_view field, std::string_view value);
    void writeLocalityIndex(std::string_view field, std::string_view hint);

    bool fieldExists(std::string_view field);
    bool localityFieldExists(std::string_view hint, std::string_view field);

    bool deleteField(std::string_view field);
    bool deleteLocalityField(std::string_view hint, std::string_view field);
    bool getAndDeleteLocalityIndex(std::string_view field, std::string &hint);

    rocksdb::Status finalize(int64_t newsize, bool forceUpdate = false);

    KeyDescriptor& descriptor() {
      return keyinfo;
    }
  private:
    StagingArea &stagingArea;
    std::string_view redisKey;

    KeyType expectedType;
    KeyDescriptor keyinfo;
    DescriptorLocator dlocator;

    bool redisKeyExists;
    bool isValid = false;
    bool finalized = false;
  };
  friend class WriteOperation;

  KeyDescriptor getKeyDescriptor(StagingArea &stagingArea, std::string_view redisKey);
  KeyDescriptor lockKeyDescriptor(StagingArea &stagingArea, DescriptorLocator &dlocator);

  void retrieveLastApplied();
  void ensureCompatibleFormat(bool justCreated);
  void ensureBulkloadSanity(bool justCreated);
  void ensureClockSanity(bool justCreated);
  void remove_all_with_prefix(std::string_view prefix, int64_t &removed, StagingArea &stagingArea);
  void lhsetInternal(WriteOperation &operation, std::string_view key, std::string_view field, std::string_view hint, std::string_view value, bool &fieldcreated);

  std::atomic<LogIndex> lastApplied;
  std::condition_variable lastAppliedCV;
  std::mutex lastAppliedMtx;

  std::mutex writeMtx;
  std::unique_ptr<rocksdb::DB> db;

  std::unique_ptr<ConsistencyScanner> consistencyScanner;

  const std::string filename;
  bool writeAheadLog;
  bool bulkLoad;

  Timekeeper timeKeeper;
  RequestCounter requestCounter;
};


}

#endif
