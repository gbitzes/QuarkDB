// ----------------------------------------------------------------------
// File: RocksDB.cc
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

#include "RocksDB.hh"
#include "Utils.hh"
#include <rocksdb/status.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/checkpoint.h>

#define RETURN_ON_ERROR(st) if(!st.ok()) return st;
#define THROW_ON_ERROR(st) if(!st.ok()) qdb_throw(st.ToString())
#define ASSERT_OK_OR_NOTFOUND(st) if(!st.ok() && !st.IsNotFound()) qdb_throw(st.ToString())

using namespace quarkdb;

RocksDB::RocksDB(const std::string &f) : filename(f) {
  qdb_info("Openning rocksdb database " << quotes(filename));

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::TransactionDB::Open(options, rocksdb::TransactionDBOptions(), filename, &transactionDB);
  if(!status.ok()) throw FatalException(status.ToString());

  db = transactionDB->GetBaseDB();
  retrieveLastApplied();
}

RocksDB::~RocksDB() {
  if(transactionDB) {
    qdb_info("Closing rocksdb database " << quotes(filename));
    delete transactionDB;
    transactionDB = nullptr;
    db = nullptr;
  }
}

void RocksDB::reset() {
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    db->Delete(rocksdb::WriteOptions(), iter->key().ToString());
  }

  retrieveLastApplied();
}

void RocksDB::retrieveLastApplied() {
  std::string tmp;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), "__last-applied", &tmp);

  if(st.ok()) {
    lastApplied = binaryStringToInt(tmp.c_str());
  }
  else if(st.IsNotFound()) {
    lastApplied = 0;
    st = db->Put(rocksdb::WriteOptions(), "__last-applied", intToBinaryString(lastApplied));
    if(!st.ok()) qdb_throw("error when setting lastApplied: " << st.ToString());
  }
  else {
    qdb_throw("error when retrieving lastApplied: " << st.ToString());
  }
}

LogIndex RocksDB::getLastApplied() {
  return lastApplied;
}

enum RedisKeyType {
  kString = 'a',
  kHash = 'b',
  kSet = 'c',
  kInternal = '_'
};

// it's rare to have to escape a key, most don't contain #
// so don't make a copy, just change the existing string
static void escape(std::string &str) {
  char replacement[3];
  replacement[0] = '|';
  replacement[1] = '#';
  replacement[2] = '\0';

  size_t pos = 0;
  while((pos = str.find('#', pos)) != std::string::npos) {
    str.replace(pos, 1, replacement);
    pos += 2;
  }
}

// given a rocksdb key (might also contain a field),
// extract the original redis key
static std::string extract_key(std::string &tkey) {
  std::string key;
  key.reserve(tkey.size());

  for(size_t i = 1; i < tkey.size(); i++) {
    // escaped hash?
    if(i != tkey.size() - 1 && tkey[i] == '|' && tkey[i+1] == '#') {
      key.append(1, '#');
      i++;
      continue;
    }
    // boundary?
    if(tkey[i] == '#') {
      break;
    }

    key.append(1, tkey[i]);
  }

  return key;
}

static std::string translate_key(const RedisKeyType type, const std::string &key) {
  std::string escaped = key;
  escape(escaped);

  return std::string(1, type) + escaped;
}

static std::string translate_key(const RedisKeyType type, const std::string &key, const std::string &field) {
  std::string translated = translate_key(type, key) + "#" + field;
  return translated;
}

rocksdb::Status RocksDB::hget(const std::string &key, const std::string &field, std::string &value) {
  std::string tkey = translate_key(kHash, key, field);
  return db->Get(rocksdb::ReadOptions(), tkey, &value);
}

rocksdb::Status RocksDB::hexists(const std::string &key, const std::string &field) {
  std::string tkey = translate_key(kHash, key, field);
  std::string value;
  return db->Get(rocksdb::ReadOptions(), tkey, &value);
}

rocksdb::Status RocksDB::hkeys(const std::string &key, std::vector<std::string> &keys) {
  std::string tkey = translate_key(kHash, key) + "#";
  keys.clear();

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    keys.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hgetall(const std::string &key, std::vector<std::string> &res) {
  std::string tkey = translate_key(kHash, key) + "#";
  res.clear();

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    res.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
    res.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hset(const std::string &key, const std::string &field, const std::string &value, LogIndex index) {
  std::string tkey = translate_key(kHash, key, field);

  TransactionPtr tx = startTransaction();
  THROW_ON_ERROR(tx->Put(tkey, value));
  commitTransaction(tx, index);
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result, LogIndex index) {
  std::string tkey = translate_key(kHash, key, field);

  TransactionPtr tx = startTransaction();

  int64_t incrbyInt64;
  if(!my_strtoll(incrby, incrbyInt64)) {
    commitTransaction(tx, index);
    return rocksdb::Status::InvalidArgument("value is not an integer or out of range");
  }

  std::string value;
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &value);
  ASSERT_OK_OR_NOTFOUND(st);

  result = 0;
  if(st.ok() && !my_strtoll(value, result)) {
    commitTransaction(tx, index);
    return rocksdb::Status::InvalidArgument("hash value is not an integer");
  }

  result += incrbyInt64;

  THROW_ON_ERROR(tx->Put(tkey, std::to_string(result)));
  commitTransaction(tx, index);
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hdel(const std::string &key, const std::string &field, LogIndex index) {
  std::string tkey = translate_key(kHash, key, field);

  TransactionPtr tx = startTransaction();

  std::string value;
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &value);

  if(st.ok()) {
    THROW_ON_ERROR(tx->Delete(tkey));
  }
  else if(!st.IsNotFound()) {
    qdb_throw(st.ToString());
  }

  commitTransaction(tx, index);
  return st;
}

rocksdb::Status RocksDB::hlen(const std::string &key, size_t &len) {
  len = 0;

  std::string tkey = translate_key(kHash, key) + "#";
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(!startswith(iter->key().ToString(), tkey)) break;
    len++;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hvals(const std::string &key, std::vector<std::string> &vals) {
  std::string tkey = translate_key(kHash, key) + "#";
  vals.clear();

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    vals.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::sadd(const std::string &key, const std::string &element, int64_t &added, LogIndex index) {
  std::string tkey = translate_key(kSet, key, element);

  TransactionPtr tx = startTransaction();

  std::string tmp;
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp);

  if(st.IsNotFound()) {
    added++;
    RETURN_ON_ERROR(tx->Put(tkey, "1"));
    st = rocksdb::Status::OK();
  }

  commitTransaction(tx, index);
  return st;
}

rocksdb::Status RocksDB::sismember(const std::string &key, const std::string &element) {
  std::string tkey = translate_key(kSet, key, element);

  std::string tmp;
  return db->Get(rocksdb::ReadOptions(), tkey, &tmp);
}

rocksdb::Status RocksDB::srem(const std::string &key, const std::string &element, LogIndex index) {
  std::string tkey = translate_key(kSet, key, element);

  TransactionPtr tx = startTransaction();

  std::string tmp;
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp);

  if(st.ok()) THROW_ON_ERROR(tx->Delete(tkey));
  commitTransaction(tx, index);
  return st;
}

rocksdb::Status RocksDB::smembers(const std::string &key, std::vector<std::string> &members) {
  std::string tkey = translate_key(kSet, key) + "#";
  members.clear();

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    members.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::scard(const std::string &key, size_t &count) {
  std::string tkey = translate_key(kSet, key) + "#";
  count = 0;

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(!startswith(iter->key().ToString(), tkey)) break;
    count++;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::set(const std::string& key, const std::string& value, LogIndex index) {
  std::string tkey = translate_key(kString, key);

  TransactionPtr tx = startTransaction();
  RETURN_ON_ERROR(tx->Put(tkey, value));
  commitTransaction(tx, index);
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::get(const std::string &key, std::string &value) {
  std::string tkey = translate_key(kString, key);
  return db->Get(rocksdb::ReadOptions(), tkey, &value);
}

// if 0 keys are found matching prefix, we return kOk, not kNotFound
rocksdb::Status RocksDB::remove_all_with_prefix(const std::string &prefix, LogIndex index) {
  std::string tmp;

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  TransactionPtr tx = startTransaction();

  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    if(!startswith(key, prefix)) break;
    RETURN_ON_ERROR(tx->Delete(key));
  }
  commitTransaction(tx, index);
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::del(const std::string &key, LogIndex index) {
  std::string tmp;

  // is it a string?
  std::string str_key = translate_key(kString, key);
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), str_key, &tmp);
  if(st.ok()) return remove_all_with_prefix(str_key, index);

  // is it a hash?
  std::string hash_key = translate_key(kHash, key) + "#";
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  iter->Seek(hash_key);
  if(iter->Valid() && startswith(iter->key().ToString(), hash_key)) {
    return remove_all_with_prefix(hash_key, index);
  }

  // is it a set?
  std::string set_key = translate_key(kSet, key) + "#";
  iter = IteratorPtr(db->NewIterator(rocksdb::ReadOptions()));
  iter->Seek(set_key);
  if(iter->Valid() && startswith(iter->key().ToString(), set_key)) {
    return remove_all_with_prefix(set_key, index);
  }

  TransactionPtr tx = startTransaction();
  commitTransaction(tx, index);
  return rocksdb::Status::NotFound();
}

rocksdb::Status RocksDB::exists(const std::string &key) {
  std::string tmp;

  // is it a string?
  std::string str_key = translate_key(kString, key);
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), str_key, &tmp);
  if(st.ok() || !st.IsNotFound()) return st;

  // is it a hash?
  std::string hash_key = translate_key(kHash, key) + "#";
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  iter->Seek(hash_key);
  if(iter->Valid() && startswith(iter->key().ToString(), hash_key)) {
    return rocksdb::Status::OK();
  }

  // is it a set?
  std::string set_key = translate_key(kSet, key) + "#";
  iter = IteratorPtr(db->NewIterator(rocksdb::ReadOptions()));
  iter->Seek(set_key);
  if(iter->Valid() && startswith(iter->key().ToString(), set_key)) {
    return rocksdb::Status::OK();
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status RocksDB::keys(const std::string &pattern, std::vector<std::string> &result) {
  result.clear();

  bool allkeys = (pattern.length() == 1 && pattern[0] == '*');
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  std::string previous;
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    std::string redis_key = extract_key(tmp);

    if(redis_key != previous && redis_key[0] != kInternal) {
      if(allkeys || stringmatchlen(pattern.c_str(), pattern.length(),
                                   redis_key.c_str(), redis_key.length(), 0)) {
        result.push_back(redis_key);
      }
    }
    previous = redis_key;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::flushall(LogIndex index) {
  return remove_all_with_prefix("", index);
}

void RocksDB::set_or_die(const std::string &key, const std::string &value) {
  rocksdb::Status st = this->set(key, value);
  if(!st.ok()) {
    throw FatalException(SSTR("unable to set key " << key << " to " << value << ". Error: " << st.ToString()));
  }
}

rocksdb::Status RocksDB::checkpoint(const std::string &path) {
  rocksdb::Checkpoint *checkpoint = nullptr;
  RETURN_ON_ERROR(rocksdb::Checkpoint::Create(db, &checkpoint));

  rocksdb::Status st = checkpoint->CreateCheckpoint(path);
  delete checkpoint;

  return st;
}


std::string RocksDB::get_or_die(const std::string &key) {
  std::string tmp;
  rocksdb::Status st = this->get(key, tmp);
  if(!st.ok()) {
    throw FatalException(SSTR("unable to get key " << key << ". Error: " << st.ToString()));
  }
  return tmp;
}

int64_t RocksDB::get_int_or_die(const std::string &key) {
  std::string tmp = this->get_or_die(key);
  return binaryStringToInt(tmp.c_str());
}

void RocksDB::set_int_or_die(const std::string &key, int64_t value) {
  rocksdb::Status st = this->set(key, intToBinaryString(value));
  if(!st.ok()) {
    throw FatalException(SSTR("unable to set key " << key << ". Error: " << st.ToString()));
  }
}

RocksDB::TransactionPtr RocksDB::startTransaction() {
  return TransactionPtr(transactionDB->BeginTransaction(rocksdb::WriteOptions()));
}

void RocksDB::commitTransaction(TransactionPtr &tx, LogIndex index) {
  if(index <= 0 && lastApplied > 0) qdb_throw("provided invalid index for version-tracked database: " << index << ", current last applied: " << lastApplied);

  if(index > 0) {
    if(index != lastApplied+1) qdb_throw("attempted to perform illegal lastApplied update: " << lastApplied << " ==> " << index);
    THROW_ON_ERROR(tx->Put("__last-applied", intToBinaryString(index)));
  }

  rocksdb::Status st = tx->Commit();
  if(index > 0 && st.ok()) lastApplied = index;

  if(!st.ok()) qdb_throw("unable to commit transaction with index " << index << ": " << st.ToString());
}
