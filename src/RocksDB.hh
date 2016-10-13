// ----------------------------------------------------------------------
// File: RocksDB.hh
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

namespace quarkdb {

class RocksDB {
public:
  RocksDB(const std::string &filename);
  virtual ~RocksDB();
  DISALLOW_COPY_AND_ASSIGN(RocksDB);

  using IteratorPtr = std::shared_ptr<rocksdb::Iterator>;

  //------------------------------------------------------------------------------
  // Main API
  //------------------------------------------------------------------------------

  rocksdb::Status hget(const std::string &key, const std::string &field, std::string &value);
  rocksdb::Status hexists(const std::string &key, const std::string &field);
  rocksdb::Status hkeys(const std::string &key, std::vector<std::string> &keys);
  rocksdb::Status hgetall(const std::string &key, std::vector<std::string> &res);
  rocksdb::Status hset(const std::string &key, const std::string &field, const std::string &value);
  rocksdb::Status hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result);
  rocksdb::Status hdel(const std::string &key, const std::string &field);
  rocksdb::Status hlen(const std::string &key, size_t &len);
  rocksdb::Status hvals(const std::string &key, std::vector<std::string> &vals);
  rocksdb::Status sadd(const std::string &key, const std::string &element, int64_t &added);
  rocksdb::Status sismember(const std::string &key, const std::string &element);
  rocksdb::Status srem(const std::string &key, const std::string &element);
  rocksdb::Status smembers(const std::string &key, std::vector<std::string> &members);
  rocksdb::Status scard(const std::string &key, size_t &count);
  rocksdb::Status set(const std::string& key, const std::string& value);
  rocksdb::Status get(const std::string &key, std::string &value);
  rocksdb::Status del(const std::string &key);
  rocksdb::Status exists(const std::string &key);
  rocksdb::Status keys(const std::string &pattern, std::vector<std::string> &result);
  rocksdb::Status flushall();

  //------------------------------------------------------------------------------
  // Convenience functions
  //------------------------------------------------------------------------------

  void set_or_die(const std::string &key, const std::string &value);
  std::string get_or_die(const std::string &key);
  int64_t get_int_or_die(const std::string &key);

private:
  rocksdb::Status remove_all_with_prefix(const std::string &prefix);

  rocksdb::DB* db = nullptr;
  const std::string filename;
};


}

#endif
