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
#include <sys/stat.h>
#include <rocksdb/status.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/checkpoint.h>

#define RETURN_ON_ERROR(st) { rocksdb::Status st2 = st; if(!st2.ok()) return st2; }
#define THROW_ON_ERROR(st) { rocksdb::Status st2 = st; if(!st2.ok()) qdb_throw(st2.ToString()); }
#define ASSERT_OK_OR_NOTFOUND(st) { rocksdb::Status st2 = st; if(!st2.ok() && !st2.IsNotFound()) qdb_throw(st2.ToString()); }

using namespace quarkdb;

static bool directoryExists(const std::string &path) {
  struct stat st;
  if(stat(path.c_str(), &st) == 0 && (st.st_mode & S_IFDIR) != 0) {
    return true;
  }

  return false;
}

RocksDB::RocksDB(const std::string &f) : filename(f) {
  qdb_info("Openning rocksdb database " << quotes(filename));
  bool dirExists = directoryExists(filename);

  rocksdb::Options options;
  options.create_if_missing = !dirExists;
  rocksdb::Status status = rocksdb::TransactionDB::Open(options, rocksdb::TransactionDBOptions(), filename, &transactionDB);
  if(!status.ok()) qdb_throw("Cannot open " << quotes(filename) << ":" << status.ToString());

  db = transactionDB->GetBaseDB();
  ensureCompatibleFormat(!dirExists);
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

  ensureCompatibleFormat(true);
  retrieveLastApplied();
}

void RocksDB::ensureCompatibleFormat(bool justCreated) {
  const std::string currentFormat("0");

  std::string format;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), "__format", &format);

  if(justCreated) {
    if(!st.IsNotFound()) qdb_throw("Error when reading __format, which should not exist: " << st.ToString());

    st = db->Put(rocksdb::WriteOptions(), "__format", currentFormat);
    if(!st.ok()) qdb_throw("error when setting format: " << st.ToString());
  }
  else {
    if(!st.ok()) qdb_throw("Cannot read __format: " << st.ToString());
    if(format != currentFormat) qdb_throw("Asked to open RocksDB store with incompatible format (" << format << "), I can only handle " << currentFormat);
  }
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
// extract the original redis key.
// Currently not needed
// static std::string extract_key(std::string &tkey) {
//   std::string key;
//   key.reserve(tkey.size());
//
//   for(size_t i = 1; i < tkey.size(); i++) {
//     // escaped hash?
//     if(i != tkey.size() - 1 && tkey[i] == '|' && tkey[i+1] == '#') {
//       key.append(1, '#');
//       i++;
//       continue;
//     }
//     // boundary?
//     if(tkey[i] == '#') {
//       break;
//     }
//
//     key.append(1, tkey[i]);
//   }
//
//   return key;
// }

static std::string translate_key(const RocksDB::KeyType type, const std::string &key) {
  std::string escaped = key;
  escape(escaped);

  return std::string(1, char(type)) + escaped;
}

static std::string translate_key(const RocksDB::KeyType type, const std::string &key, const std::string &field) {
  std::string translated = translate_key(type, key) + "#" + field;
  return translated;
}

static rocksdb::Status wrong_type() {
  return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
}

std::string RocksDB::KeyDescriptor::serialize() const {
  return SSTR(char(this->keytype) << "-" << intToBinaryString(this->size));
}

RocksDB::KeyDescriptor RocksDB::KeyDescriptor::construct(const rocksdb::Status &st, const std::string &str, std::string &&dkey) {
  KeyDescriptor keyinfo;
  keyinfo.dkey = std::move(dkey);

  if(st.IsNotFound()) {
    keyinfo.exists = false;
    keyinfo.size = 0;
    return keyinfo;
  }

  if(!st.ok()) qdb_throw("unexpected rocksdb status when inspecting KeyType entry " << keyinfo.dkey << ": " << st.ToString());
  keyinfo.exists = true;
  if(str.size() != 10) qdb_throw("unable to parse keyInfo, unexpected size, was expecting 10: " << str);

  if(str[0] == char(KeyType::kString)) {
    keyinfo.keytype = KeyType::kString;
  }
  else if(str[0] == char(KeyType::kHash)) {
    keyinfo.keytype = KeyType::kHash;
  }
  else if(str[0] == char(KeyType::kSet)) {
    keyinfo.keytype = KeyType::kSet;
  }
  else {
    qdb_throw("unable to parse keyInfo, unknown key type: '" << str[0] << "'");
  }

  if(str[1] != '-') qdb_throw("unable to parse keyInfo, unexpected char for key type descriptor, expected '-': " << str[1]);
  keyinfo.size = binaryStringToInt(str.c_str()+2);
  return keyinfo;
}

RocksDB::KeyDescriptor RocksDB::getKeyDescriptor(const std::string &redisKey) {
  std::string tmp;
  std::string dkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), dkey, &tmp);
  return KeyDescriptor::construct(st, tmp, std::move(dkey));
}

RocksDB::KeyDescriptor RocksDB::getKeyDescriptor(RocksDB::Snapshot &snapshot, const std::string &redisKey) {
  std::string tmp;
  std::string dkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = db->Get(snapshot.opts(), dkey, &tmp);
  return KeyDescriptor::construct(st, tmp, std::move(dkey));
}

RocksDB::KeyDescriptor RocksDB::lockKeyDescriptor(TransactionPtr &tx, const std::string &redisKey) {
  std::string tmp, tkey;
  tkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp);
  return KeyDescriptor::construct(st, tmp, std::move(tkey));
}

bool RocksDB::assertKeyType(Snapshot &snapshot, const std::string &key, KeyType keytype) {
  KeyDescriptor keyinfo = getKeyDescriptor(snapshot, key);
  if(keyinfo.exists && keyinfo.keytype != keytype) return false;
  return true;
}

rocksdb::Status RocksDB::hget(const std::string &key, const std::string &field, std::string &value) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  std::string tkey = translate_key(KeyType::kHash, key, field);
  return db->Get(snapshot.opts(), tkey, &value);
}

rocksdb::Status RocksDB::hexists(const std::string &key, const std::string &field) {
  std::string tmp;
  return this->hget(key, field, tmp);
}

rocksdb::Status RocksDB::hkeys(const std::string &key, std::vector<std::string> &keys) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  std::string tkey = translate_key(KeyType::kHash, key) + "#";
  keys.clear();

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    keys.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hgetall(const std::string &key, std::vector<std::string> &res) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  std::string tkey = translate_key(KeyType::kHash, key) + "#";
  res.clear();

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    res.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
    res.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}


rocksdb::Status RocksDB::wrongKeyType(TransactionPtr &tx, LogIndex index) {
  commitTransaction(tx, index);
  return wrong_type();
}

rocksdb::Status RocksDB::hset(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  operation.writeField(field, value);
  operation.finalize(newsize);

  return finalize(tx, index);
}

rocksdb::Status RocksDB::hsetnx(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  if(fieldcreated) {
    operation.writeField(field, value);
  }

  operation.finalize(newsize);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result, LogIndex index) {
  TransactionPtr tx = startTransaction();

  int64_t incrbyInt64;
  if(!my_strtoll(incrby, incrbyInt64)) {
    return malformedRequest(tx, index, "value is not an integer or out of range");
  }

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  std::string tkey = translate_key(KeyType::kHash, key, field);

  std::string value;
  bool exists = operation.getField(field, value);

  result = 0;
  if(exists && !my_strtoll(value, result)) {
    operation.finalize(operation.keySize());
    return malformedRequest(tx, index, "hash value is not an integer");
  }

  result += incrbyInt64;

  operation.writeField(field, std::to_string(result));
  operation.finalize(operation.keySize() + !exists);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::hincrbyfloat(const std::string &key, const std::string &field, const std::string &incrby, double &result, LogIndex index) {
  TransactionPtr tx = startTransaction();

  double incrByDouble;
  if(!my_strtod(incrby, incrByDouble)) {
    return malformedRequest(tx, index, "value is not a float or out of range");
  }

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  std::string tkey = translate_key(KeyType::kHash, key, field);

  std::string value;
  bool exists = operation.getField(field, value);

  result = 0;
  if(exists && !my_strtod(value, result)) {
    operation.finalize(operation.keySize());
    return malformedRequest(tx, index, "hash value is not a float");
  }

  result += incrByDouble;

  operation.writeField(field, std::to_string(result));
  operation.finalize(operation.keySize() + !exists);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::hdel(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  removed = 0;
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  for(VecIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  int64_t newsize = operation.keySize() - removed;
  operation.finalize(newsize);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::hlen(const std::string &key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists && keyinfo.keytype != KeyType::kHash) return wrong_type();

  len = keyinfo.size;
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hscan(const std::string &key, const std::string &cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  std::string prefix = translate_key(KeyType::kHash, key) + "#";
  std::string tkey = translate_key(KeyType::kHash, key, cursor);
  res.clear();

  newCursor = "";
  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();

    if(!startswith(tmp, prefix)) break;

    std::string fieldname = std::string(tmp.begin()+prefix.size(), tmp.end());
    if(res.size() >= count*2) {
      newCursor = fieldname;
      break;
    }

    res.push_back(fieldname);
    res.push_back(iter->value().ToString());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::hvals(const std::string &key, std::vector<std::string> &vals) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  std::string tkey = translate_key(KeyType::kHash, key) + "#";
  vals.clear();

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    vals.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::sadd(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &added, LogIndex index) {
  added = 0;
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kSet);
  if(!operation.valid()) return wrongKeyType(tx, index);

  for(VecIterator it = start; it != end; it++) {
    bool exists = operation.fieldExists(*it);
    if(!exists) {
      operation.writeField(*it, "1");
      added++;
    }
  }

  operation.finalize(operation.keySize() + added);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::sismember(const std::string &key, const std::string &element) {
  Snapshot snapshot(db);
  KeyDescriptor keyinfo = getKeyDescriptor(snapshot, key);
  if(keyinfo.exists && keyinfo.keytype != KeyType::kSet) return wrong_type();
  std::string tkey = translate_key(KeyType::kSet, key, element);

  std::string tmp;
  return db->Get(snapshot.opts(), tkey, &tmp);
}

rocksdb::Status RocksDB::srem(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  removed = 0;
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kSet);
  if(!operation.valid()) return wrongKeyType(tx, index);

  for(VecIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  operation.finalize(operation.keySize() - removed);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::smembers(const std::string &key, std::vector<std::string> &members) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kSet)) return wrong_type();

  std::string tkey = translate_key(KeyType::kSet, key) + "#";
  members.clear();

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    members.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::scard(const std::string &key, size_t &count) {
  count = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists && keyinfo.keytype != KeyType::kSet) return wrong_type();

  count = keyinfo.size;
  return rocksdb::Status::OK();
}

RocksDB::WriteOperation::~WriteOperation() {
  if(!finalized) {
    std::cerr << "WriteOperation being destroyed without having been finalized" << std::endl;
    std::terminate();
  }
}

RocksDB::WriteOperation::WriteOperation(RocksDB::TransactionPtr &tx_, const std::string &key, const KeyType &type)
: tx(tx_), redisKey(key), expectedType(type) {

  std::string tmp, dkey;
  dkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), dkey, &tmp);
  keyinfo = KeyDescriptor::construct(st, tmp, std::move(dkey));

  redisKeyExists = keyinfo.exists;
  isValid = (!keyinfo.exists) || (keyinfo.keytype == type);

  if(!keyinfo.exists && isValid) {
    keyinfo.keytype = expectedType;
  }

  finalized = !isValid;
}

bool RocksDB::WriteOperation::valid() {
  return isValid;
}

bool RocksDB::WriteOperation::keyExists() {
  return redisKeyExists;
}

bool RocksDB::WriteOperation::getField(const std::string &field, std::string &out) {
  assertWritable();

  std::string tkey = translate_key(keyinfo.keytype, redisKey, field);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &out);
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

int64_t RocksDB::WriteOperation::keySize() {
  return keyinfo.size;
}

void RocksDB::WriteOperation::assertWritable() {
  if(!isValid) qdb_throw("WriteOperation not valid!");
  if(finalized) qdb_throw("WriteOperation already finalized!");
}

void RocksDB::WriteOperation::write(const std::string &value) {
  assertWritable();

  if(keyinfo.keytype != KeyType::kString) {
    qdb_throw("writing without a field makes sense only for strings");
  }

  std::string tkey = translate_key(keyinfo.keytype, redisKey);
  THROW_ON_ERROR(tx->Put(tkey, value));
}

void RocksDB::WriteOperation::writeField(const std::string &field, const std::string &value) {
  assertWritable();

  if(keyinfo.keytype != KeyType::kHash && keyinfo.keytype != KeyType::kSet) {
    qdb_throw("writing with a field makes sense only for hashes and sets");
  }

  std::string tkey = translate_key(keyinfo.keytype, redisKey, field);
  THROW_ON_ERROR(tx->Put(tkey, value));
}

void RocksDB::WriteOperation::finalize(int64_t newsize) {
  assertWritable();

  if(newsize < 0) qdb_throw("invalid newsize: " << newsize);
  keyinfo.size = newsize;

  if(newsize == 0) {
    THROW_ON_ERROR(tx->Delete(keyinfo.dkey));
  }
  else {
    THROW_ON_ERROR(tx->Put(keyinfo.dkey, keyinfo.serialize()));
  }

  finalized = true;
}

bool RocksDB::WriteOperation::fieldExists(const std::string &field) {
  assertWritable();

  std::string tmp;
  return getField(field, tmp);
}

bool RocksDB::WriteOperation::deleteField(const std::string &field) {
  assertWritable();

  std::string tmp;
  std::string tkey = translate_key(keyinfo.keytype, redisKey, field);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) THROW_ON_ERROR(tx->Delete(tkey));
  return st.ok();
}

rocksdb::Status RocksDB::set(const std::string& key, const std::string& value, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kString);
  if(!operation.valid()) return wrongKeyType(tx, index);

  operation.write(value);
  operation.finalize(value.size());

  return finalize(tx, index);
}

RocksDB::Snapshot::Snapshot(rocksdb::DB *db_) {
  db = db_;
  snapshot = db->GetSnapshot();
  if(snapshot == nullptr) qdb_throw("unable to take db snapshot");
  options.snapshot = snapshot;
}

RocksDB::Snapshot::~Snapshot() {
  db->ReleaseSnapshot(snapshot);
}

rocksdb::ReadOptions& RocksDB::Snapshot::opts() {
  return options;
}

rocksdb::Status RocksDB::get(const std::string &key, std::string &value) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kString)) return wrong_type();

  std::string tkey = translate_key(KeyType::kString, key);
  return db->Get(snapshot.opts(), tkey, &value);
}

void RocksDB::remove_all_with_prefix(const std::string &prefix, int64_t &removed, TransactionPtr &tx) {
  removed = 0;

  std::string tmp;
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));

  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    if(!startswith(key, prefix)) break;
    if(key.size() > 0 && key[0] == char(InternalKeyType::kInternal)) continue;

    THROW_ON_ERROR(tx->Delete(key));
    removed++;
  }
}

rocksdb::Status RocksDB::del(const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  removed = 0;
  TransactionPtr tx = startTransaction();

  for(VecIterator it = start; it != end; it++) {
    KeyDescriptor keyInfo = lockKeyDescriptor(tx, *it);
    if(!keyInfo.exists) continue;

    std::string tkey, tmp;

    if(keyInfo.keytype == KeyType::kString) {
      tkey = translate_key(KeyType::kString, *it);
      THROW_ON_ERROR(tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp));
      THROW_ON_ERROR(tx->Delete(tkey));
    }
    else if(keyInfo.keytype == KeyType::kHash || keyInfo.keytype == KeyType::kSet) {
      tkey = translate_key(keyInfo.keytype, *it) + "#";
      int64_t count = 0;
      remove_all_with_prefix(tkey, count, tx);
      if(count != keyInfo.size) qdb_throw("mismatch between keyInfo counter and number of elements deleted by remove_all_with_prefix: " << count << " vs " << keyInfo.size);
    }
    else {
      qdb_throw("should never happen");
    }

    removed++;
    THROW_ON_ERROR(tx->Delete(keyInfo.dkey));
  }

  return finalize(tx, index);
}

rocksdb::Status RocksDB::exists(const std::string &key) {
  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists) return rocksdb::Status::OK();
  return rocksdb::Status::NotFound();
}

rocksdb::Status RocksDB::keys(const std::string &pattern, std::vector<std::string> &result) {
  result.clear();

  bool allkeys = (pattern.length() == 1 && pattern[0] == '*');
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));

  std::string searchPrefix(1, char(InternalKeyType::kDescriptor));
  for(iter->Seek(searchPrefix); iter->Valid(); iter->Next()) {
    std::string rkey = iter->key().ToString();
    if(rkey.size() == 0 || rkey[0] != char(InternalKeyType::kDescriptor)) break;

    if(allkeys || stringmatchlen(pattern.c_str(), pattern.length(), rkey.c_str()+1, rkey.length()-1, 0)) {
      result.push_back(rkey.substr(1));
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::flushall(LogIndex index) {
  int64_t tmp;
  TransactionPtr tx = startTransaction();
  remove_all_with_prefix("", tmp, tx);
  return finalize(tx, index);
}

rocksdb::Status RocksDB::checkpoint(const std::string &path) {
  rocksdb::Checkpoint *checkpoint = nullptr;
  RETURN_ON_ERROR(rocksdb::Checkpoint::Create(db, &checkpoint));

  rocksdb::Status st = checkpoint->CreateCheckpoint(path);
  delete checkpoint;

  return st;
}

RocksDB::TransactionPtr RocksDB::startTransaction() {
  return TransactionPtr(transactionDB->BeginTransaction(rocksdb::WriteOptions()));
}

rocksdb::Status RocksDB::noop(LogIndex index) {
  TransactionPtr tx = startTransaction();
  return finalize(tx, index);
}

rocksdb::Status RocksDB::finalize(TransactionPtr &tx, LogIndex index) {
  commitTransaction(tx, index);
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDB::malformedRequest(TransactionPtr &tx, LogIndex index, std::string message) {
  commitTransaction(tx, index);
  return rocksdb::Status::InvalidArgument(message);
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
