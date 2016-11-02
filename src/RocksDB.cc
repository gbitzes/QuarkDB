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

using namespace quarkdb;

// merge operator for additions to provide atomic incrby
// IF YOU CHANGE THIS ALL PREVIOUS DATABASES BECOME INCOMPATIBLE !!!!!!
class Int64AddOperator : public rocksdb::AssociativeMergeOperator {
public:
  virtual bool Merge(const rocksdb::Slice& key, const rocksdb::Slice* existing_value,
                     const rocksdb::Slice& value, std::string* new_value,
                     rocksdb::Logger* logger) const override {
    // there's no decent way to do error reporting to the client
    // inside a rocksdb Merge operator, partially also because
    // the method is applied asynchronously and might not be run
    // until the next Get on this key!!
    //
    // ignore all errors and return true, without modifying the value.
    // returning false here corrupts the key entirely!
    // all sanity checking should be done in the client code calling merge

    // assuming 0 if no existing value
    int64_t existing = 0;
    if(existing_value) {
      if(!my_strtoll(existing_value->ToString(), existing)) {
        *new_value = existing_value->ToString();
        return true;
      }
    }

    int64_t oper;
    if(!my_strtoll(value.ToString(), oper)) {
      // this should not happen under any circumstances..
      *new_value = existing_value->ToString();
      return true;
    }

    int64_t newval = existing + oper;
    std::stringstream ss;
    ss << newval;
    *new_value = ss.str();
    return true;
  }

  virtual const char* Name() const override {
    return "Int64AddOperator";
  }
};

RocksDB::RocksDB(const std::string &f) : filename(f) {
  qdb_info("Openning rocksdb database " << quotes(filename));

  rocksdb::Options options;
  options.merge_operator.reset(new Int64AddOperator);
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, filename, &db);
  if(!status.ok()) throw FatalException(status.ToString());
}

RocksDB::~RocksDB() {
  if(db) {
    qdb_info("Closing rocksdb database " << quotes(filename));
    delete db;
    db = nullptr;
  }
}

enum RedisCommandType {
  kString = 'a',
  kHash,
  kSet
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

static std::string translate_key(const RedisCommandType type, const std::string &key) {
  std::string escaped = key;
  escape(escaped);

  return std::string(1, type) + escaped;
}

static std::string translate_key(const RedisCommandType type, const std::string &key, const std::string &field) {
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

rocksdb::Status RocksDB::hset(const std::string &key, const std::string &field, const std::string &value) {
  std::string tkey = translate_key(kHash, key, field);
  return db->Put(rocksdb::WriteOptions(), tkey, value);
}

rocksdb::Status RocksDB::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  std::string tkey = translate_key(kHash, key, field);

  int64_t tmp;
  if(!my_strtoll(incrby, tmp)) {
    return rocksdb::Status::InvalidArgument("value is not an integer or out of range");
  }

  rocksdb::Status st = db->Merge(rocksdb::WriteOptions(), tkey, incrby);
  if(!st.ok()) return st;

  std::string value;
  st = db->Get(rocksdb::ReadOptions(), tkey, &value);

  if(!my_strtoll(value, result)) {
    // This can occur under two circumstances: The value in tkey was not an
    // integer in the first place, and the Merge operation had no effects on it.

    // It could also happen if the Merge operation was successful, but then
    // afterwards another request came up and set tkey to a non-integer.
    // Even in this case the redis semantics are not violated - we just pretend
    // this request was processed after the other thread modified the key to a
    // non-integer.

    return rocksdb::Status::InvalidArgument("hash value is not an integer");
  }

  // RACE CONDITION: An OK() can be erroneous in the following scenario:
  // original value was "aaa"
  // HINCRBY called to increase by 1
  // Merge operation failed and did not modify the value at all
  // Another thread came by and set the value to "5"
  // Now, this thread sees an integer and thinks its merge operation was successful,
  // happily reporting "5" to the user.
  //
  // Unfortunately, the semantics of rocksdb makes this very difficult to avoid
  // without an extra layer of synchronization on top of it..

  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hdel(const std::string &key, const std::string &field) {
  std::string tkey = translate_key(kHash, key, field);

  // race condition
  std::string value;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), tkey, &value);
  if(!st.ok()) return st;

  return db->Delete(rocksdb::WriteOptions(), tkey);
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

rocksdb::Status RocksDB::sadd(const std::string &key, const std::string &element, int64_t &added) {
  std::string tkey = translate_key(kSet, key, element);

  std::string tmp;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), tkey, &tmp);
  if(st.IsNotFound()) {
    added++;
    return db->Put(rocksdb::WriteOptions(), tkey, "1");
  }

  return st;
}

rocksdb::Status RocksDB::sismember(const std::string &key, const std::string &element) {
  std::string tkey = translate_key(kSet, key, element);

  std::string tmp;
  return db->Get(rocksdb::ReadOptions(), tkey, &tmp);
}

rocksdb::Status RocksDB::srem(const std::string &key, const std::string &element) {
  std::string tkey = translate_key(kSet, key, element);

  // race condition
  std::string value;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), tkey, &value);
  if(!st.ok()) return st;

  return db->Delete(rocksdb::WriteOptions(), tkey);
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

rocksdb::Status RocksDB::set(const std::string& key, const std::string& value) {
  std::string tkey = translate_key(kString, key);
  return db->Put(rocksdb::WriteOptions(), tkey, value);
}

rocksdb::Status RocksDB::get(const std::string &key, std::string &value) {
  std::string tkey = translate_key(kString, key);
  return db->Get(rocksdb::ReadOptions(), tkey, &value);
}

// if 0 keys are found matching prefix, we return kOk, not kNotFound
rocksdb::Status RocksDB::remove_all_with_prefix(const std::string &prefix) {
  std::string tmp;

  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    if(!startswith(key, prefix)) break;
    rocksdb::Status st = db->Delete(rocksdb::WriteOptions(), key);
    if(!st.ok()) return st;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::del(const std::string &key) {
  std::string tmp;

  // is it a string?
  std::string str_key = translate_key(kString, key);
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), str_key, &tmp);
  if(st.ok()) return remove_all_with_prefix(str_key);

  // is it a hash?
  std::string hash_key = translate_key(kHash, key) + "#";
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  iter->Seek(hash_key);
  if(iter->Valid() && startswith(iter->key().ToString(), hash_key)) {
    return remove_all_with_prefix(hash_key);
  }

  // is it a set?
  std::string set_key = translate_key(kSet, key) + "#";
  iter = IteratorPtr(db->NewIterator(rocksdb::ReadOptions()));
  iter->Seek(set_key);
  if(iter->Valid() && startswith(iter->key().ToString(), set_key)) {
    return remove_all_with_prefix(set_key);
  }
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

    if(redis_key != previous) {
      if(allkeys || stringmatchlen(pattern.c_str(), pattern.length(),
                                   redis_key.c_str(), redis_key.length(), 0)) {
        result.push_back(redis_key);
      }
    }
    previous = redis_key;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::flushall() {
  return remove_all_with_prefix("");
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

  int64_t value;
  if(!my_strtoll(tmp, value)) {
    throw FatalException(SSTR("db corruption, unable to parse integer key " << key << ". Received " << value));
  }

  return value;
}
