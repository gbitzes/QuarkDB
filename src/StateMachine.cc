// ----------------------------------------------------------------------
// File: StateMachine.cc
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

#include "StateMachine.hh"
#include "Utils.hh"
#include <sys/stat.h>
#include <rocksdb/status.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

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

StateMachine::StateMachine(const std::string &f, bool write_ahead_log)
: filename(f), writeAheadLog(write_ahead_log) {

  if(writeAheadLog) {
    qdb_info("Openning state machine " << quotes(filename) << ".");
  }
  else {
    qdb_warn("Opening state machine " << quotes(filename) << " *without* write ahead log - an unclean shutdown WILL CAUSE DATA LOSS");
  }

  bool dirExists = directoryExists(filename);

  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(32, false));
  table_options.block_size = 16 * 1024;

  options.create_if_missing = !dirExists;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  rocksdb::TransactionDBOptions txopts;
  txopts.transaction_lock_timeout = -1;
  txopts.default_lock_timeout = -1;

  rocksdb::Status status = rocksdb::TransactionDB::Open(options, txopts, filename, &transactionDB);
  if(!status.ok()) qdb_throw("Cannot open " << quotes(filename) << ":" << status.ToString());

  db = transactionDB->GetBaseDB();
  ensureCompatibleFormat(!dirExists);
  retrieveLastApplied();
}

StateMachine::~StateMachine() {
  if(transactionDB) {
    qdb_info("Closing state machine " << quotes(filename));
    delete transactionDB;
    transactionDB = nullptr;
    db = nullptr;
  }
}

void StateMachine::reset() {
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    db->Delete(rocksdb::WriteOptions(), iter->key().ToString());
  }

  ensureCompatibleFormat(true);
  retrieveLastApplied();
}

void StateMachine::ensureCompatibleFormat(bool justCreated) {
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
    if(format != currentFormat) qdb_throw("Asked to open a state machine with incompatible format (" << format << "), I can only handle " << currentFormat);
  }
}

void StateMachine::retrieveLastApplied() {
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

LogIndex StateMachine::getLastApplied() {
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

static std::string translate_key(const StateMachine::KeyType type, const std::string &key) {
  std::string escaped = key;
  escape(escaped);

  return std::string(1, char(type)) + escaped;
}

static std::string translate_key(const StateMachine::KeyType type, const std::string &key, const std::string &field) {
  std::string translated = translate_key(type, key) + "#" + field;
  return translated;
}

static rocksdb::Status wrong_type() {
  return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
}

std::string StateMachine::KeyDescriptor::serialize() const {
  if(this->keytype == KeyType::kNull) {
    qdb_throw("attempted to serialize null descriptor");
  }
  else if(this->keytype == KeyType::kList) {
    return SSTR(char(this->keytype) << "-" << intToBinaryString(this->size_) << "-" << unsignedIntToBinaryString(this->listStartIndex) << "-" << unsignedIntToBinaryString(this->listEndIndex));
  }
  else {
    return SSTR(char(this->keytype) << "-" << intToBinaryString(this->size_));
  }
}

StateMachine::KeyDescriptor StateMachine::KeyDescriptor::construct(const rocksdb::Status &st, const std::string &str, std::string &&dkey) {
  KeyDescriptor keyinfo;
  keyinfo.dkey = std::move(dkey);

  if(st.IsNotFound()) {
    keyinfo.existence = false;
    keyinfo.size_ = 0;
    return keyinfo;
  }

  if(!st.ok()) qdb_throw("unexpected rocksdb status when inspecting KeyType entry " << keyinfo.dkey << ": " << st.ToString());
  keyinfo.existence = true;
  qdb_assert(!str.empty());

  if(str[0] == char(KeyType::kString)) {
    keyinfo.keytype = KeyType::kString;
  }
  else if(str[0] == char(KeyType::kHash)) {
    keyinfo.keytype = KeyType::kHash;
  }
  else if(str[0] == char(KeyType::kSet)) {
    keyinfo.keytype = KeyType::kSet;
  }
  else if(str[0] == char(KeyType::kList)) {
    keyinfo.keytype = KeyType::kList;
  }
  else {
    qdb_throw("unable to parse keyInfo, unknown key type: '" << str[0] << "'");
  }

  if(keyinfo.keytype == KeyType::kList) {
    qdb_assert(str.size() == 28);
  }
  else {
    qdb_assert(str.size() == 10);
  }

  if(str[1] != '-') qdb_throw("unable to parse keyInfo, unexpected char for key type descriptor, expected '-': " << str[1]);
  keyinfo.size_ = binaryStringToInt(str.c_str()+2);

  if(keyinfo.keytype == KeyType::kList) {
    if(str[10] != '-') qdb_throw("unable to parse keyInfo, unexpected char for key type descriptor, expected '-': " << str[10]);
    if(str[19] != '-') qdb_throw("unable to parse keyInfo, unexpected char for key type descriptor, expected '-': " << str[19]);

    keyinfo.listStartIndex = binaryStringToUnsignedInt(str.c_str()+11);
    keyinfo.listEndIndex = binaryStringToUnsignedInt(str.c_str()+20);
    qdb_assert(keyinfo.listStartIndex <= keyinfo.listEndIndex);
  }
  return keyinfo;
}

StateMachine::KeyDescriptor StateMachine::getKeyDescriptor(const std::string &redisKey) {
  std::string tmp;
  std::string dkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), dkey, &tmp);
  return KeyDescriptor::construct(st, tmp, std::move(dkey));
}

StateMachine::KeyDescriptor StateMachine::getKeyDescriptor(StateMachine::Snapshot &snapshot, const std::string &redisKey) {
  std::string tmp;
  std::string dkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = db->Get(snapshot.opts(), dkey, &tmp);
  return KeyDescriptor::construct(st, tmp, std::move(dkey));
}

StateMachine::KeyDescriptor StateMachine::lockKeyDescriptor(TransactionPtr &tx, const std::string &redisKey) {
  std::string tmp, tkey;
  tkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp);
  return KeyDescriptor::construct(st, tmp, std::move(tkey));
}

bool StateMachine::assertKeyType(Snapshot &snapshot, const std::string &key, KeyType keytype) {
  KeyDescriptor keyinfo = getKeyDescriptor(snapshot, key);
  if(keyinfo.exists() && keyinfo.type() != keytype) return false;
  return true;
}

rocksdb::Status StateMachine::hget(const std::string &key, const std::string &field, std::string &value) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  std::string tkey = translate_key(KeyType::kHash, key, field);
  return db->Get(snapshot.opts(), tkey, &value);
}

rocksdb::Status StateMachine::hexists(const std::string &key, const std::string &field) {
  std::string tmp;
  return this->hget(key, field, tmp);
}

rocksdb::Status StateMachine::hkeys(const std::string &key, std::vector<std::string> &keys) {
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

rocksdb::Status StateMachine::hgetall(const std::string &key, std::vector<std::string> &res) {
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


rocksdb::Status StateMachine::wrongKeyType(TransactionPtr &tx, LogIndex index) {
  commitTransaction(tx, index);
  return wrong_type();
}

rocksdb::Status StateMachine::hset(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  operation.writeField(field, value);
  operation.finalize(newsize);

  return finalize(tx, index);
}

rocksdb::Status StateMachine::hmset(const std::string &key, const VecIterator &start, const VecIterator &end, LogIndex index) {
  if((end - start) % 2 != 0) qdb_throw("hmset: distance between start and end iterators must be an even number");
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kHash);
  if(!operation.valid()) return wrongKeyType(tx, index);

  int64_t newsize = operation.keySize();
  for(VecIterator it = start; it != end; it += 2) {
    newsize += !operation.fieldExists(*it);
    operation.writeField(*it, *(it+1));
  }

  operation.finalize(newsize);
  return finalize(tx, index);
}

rocksdb::Status StateMachine::hsetnx(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index) {
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

rocksdb::Status StateMachine::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result, LogIndex index) {
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

rocksdb::Status StateMachine::hincrbyfloat(const std::string &key, const std::string &field, const std::string &incrby, double &result, LogIndex index) {
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

rocksdb::Status StateMachine::hdel(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
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

rocksdb::Status StateMachine::hlen(const std::string &key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists() && keyinfo.type() != KeyType::kHash) return wrong_type();

  len = keyinfo.size();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hscan(const std::string &key, const std::string &cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
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

rocksdb::Status StateMachine::hvals(const std::string &key, std::vector<std::string> &vals) {
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

rocksdb::Status StateMachine::sadd(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &added, LogIndex index) {
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

rocksdb::Status StateMachine::sismember(const std::string &key, const std::string &element) {
  Snapshot snapshot(db);
  KeyDescriptor keyinfo = getKeyDescriptor(snapshot, key);
  if(keyinfo.exists() && keyinfo.type() != KeyType::kSet) return wrong_type();
  std::string tkey = translate_key(KeyType::kSet, key, element);

  std::string tmp;
  return db->Get(snapshot.opts(), tkey, &tmp);
}

rocksdb::Status StateMachine::srem(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
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

rocksdb::Status StateMachine::smembers(const std::string &key, std::vector<std::string> &members) {
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

rocksdb::Status StateMachine::scard(const std::string &key, size_t &count) {
  count = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists() && keyinfo.type() != KeyType::kSet) return wrong_type();

  count = keyinfo.size();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::configGet(const std::string &key, std::string &value) {
  Snapshot snapshot(db);
  std::string tkey = translate_key(KeyType::kConfiguration, key);
  return db->Get(snapshot.opts(), tkey, &value);
}

rocksdb::Status StateMachine::configSet(const std::string &key, const std::string &value, LogIndex index) {
  // We don't use WriteOperation or key descriptors here,
  // since kConfiguration is special.

  std::string tkey = translate_key(KeyType::kConfiguration, key);

  TransactionPtr tx = startTransaction();
  THROW_ON_ERROR(tx->Put(tkey, value));
  commitTransaction(tx, index);

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::configGetall(std::vector<std::string> &res) {
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  res.clear();

  std::string searchPrefix(1, char(KeyType::kConfiguration));
  for(iter->Seek(searchPrefix); iter->Valid(); iter->Next()) {
    std::string rkey = iter->key().ToString();
    if(rkey.size() == 0 || rkey[0] != char(KeyType::kConfiguration)) break;

    res.push_back(rkey.substr(1));
    res.push_back(iter->value().ToString());
  }

  return rocksdb::Status::OK();
}

StateMachine::WriteOperation::~WriteOperation() {
  if(!finalized) {
    std::cerr << "WriteOperation being destroyed without having been finalized" << std::endl;
    std::terminate();
  }
}

StateMachine::WriteOperation::WriteOperation(StateMachine::TransactionPtr &tx_, const std::string &key, const KeyType &type)
: tx(tx_), redisKey(key), expectedType(type) {

  std::string tmp, dkey;
  dkey = SSTR(char(InternalKeyType::kDescriptor) << redisKey);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), dkey, &tmp);
  keyinfo = KeyDescriptor::construct(st, tmp, std::move(dkey));

  redisKeyExists = keyinfo.exists();
  isValid = (!keyinfo.exists()) || (keyinfo.type() == type);

  if(!keyinfo.exists() && isValid) {
    keyinfo.initializeType(expectedType);
  }

  finalized = !isValid;
}

bool StateMachine::WriteOperation::valid() {
  return isValid;
}

bool StateMachine::WriteOperation::keyExists() {
  return redisKeyExists;
}

bool StateMachine::WriteOperation::getField(const std::string &field, std::string &out) {
  assertWritable();

  std::string tkey = translate_key(keyinfo.type(), redisKey, field);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &out);
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

int64_t StateMachine::WriteOperation::keySize() {
  return keyinfo.size();
}

void StateMachine::WriteOperation::assertWritable() {
  if(!isValid) qdb_throw("WriteOperation not valid!");
  if(finalized) qdb_throw("WriteOperation already finalized!");
}

void StateMachine::WriteOperation::write(const std::string &value) {
  assertWritable();

  if(keyinfo.type() != KeyType::kString) {
    qdb_throw("writing without a field makes sense only for strings");
  }

  std::string tkey = translate_key(keyinfo.type(), redisKey);
  THROW_ON_ERROR(tx->Put(tkey, value));
}

void StateMachine::WriteOperation::writeField(const std::string &field, const std::string &value) {
  assertWritable();

  if(keyinfo.type() != KeyType::kHash && keyinfo.type() != KeyType::kSet && keyinfo.type() != KeyType::kList) {
    qdb_throw("writing with a field makes sense only for hashes, sets, or lists");
  }

  std::string tkey = translate_key(keyinfo.type(), redisKey, field);
  THROW_ON_ERROR(tx->Put(tkey, value));
}

void StateMachine::WriteOperation::finalize(int64_t newsize) {
  assertWritable();

  if(newsize < 0) qdb_throw("invalid newsize: " << newsize);

  if(newsize == 0) {
    THROW_ON_ERROR(tx->Delete(keyinfo.getRawKey()));
  }
  else if(keyinfo.size() != newsize) {
    keyinfo.setSize(newsize);
    THROW_ON_ERROR(tx->Put(keyinfo.getRawKey(), keyinfo.serialize()));
  }

  finalized = true;
}

bool StateMachine::WriteOperation::fieldExists(const std::string &field) {
  assertWritable();

  std::string tmp;
  return getField(field, tmp);
}

bool StateMachine::WriteOperation::deleteField(const std::string &field) {
  assertWritable();

  std::string tmp;
  std::string tkey = translate_key(keyinfo.type(), redisKey, field);
  rocksdb::Status st = tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) THROW_ON_ERROR(tx->Delete(tkey));
  return st.ok();
}

rocksdb::Status StateMachine::set(const std::string& key, const std::string& value, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kString);
  if(!operation.valid()) return wrongKeyType(tx, index);

  operation.write(value);
  operation.finalize(value.size());

  return finalize(tx, index);
}

rocksdb::Status StateMachine::listPush(Direction direction, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kList);
  if(!operation.valid()) return wrongKeyType(tx, index);

  KeyDescriptor &descriptor = operation.descriptor();

  uint64_t listIndex = descriptor.getListIndex(direction);
  uint64_t itemsAdded = 0;
  for(VecIterator it = start; it != end; it++) {
    operation.writeField(unsignedIntToBinaryString(listIndex + (itemsAdded*(int)direction)), *it);
    itemsAdded++;
  }

  descriptor.setListIndex(direction, listIndex + (itemsAdded*(int)direction));
  length = operation.keySize() + itemsAdded;
  if(operation.keySize() == 0) {
    descriptor.setListIndex(flipDirection(direction), listIndex + ((int)direction*-1));
  }
  operation.finalize(length);
  return finalize(tx, index);
}

rocksdb::Status StateMachine::llen(const std::string &key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists() && keyinfo.type() != KeyType::kList) return wrong_type();

  len = keyinfo.size();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::lpush(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index) {
  return listPush(Direction::kLeft, key, start, end, length, index);
}

rocksdb::Status StateMachine::rpush(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index) {
  return listPush(Direction::kRight, key, start, end, length, index);
}

rocksdb::Status StateMachine::listPop(Direction direction, const std::string &key, std::string &item, LogIndex index) {
  TransactionPtr tx = startTransaction();

  WriteOperation operation(tx, key, KeyType::kList);
  if(!operation.valid()) return wrongKeyType(tx, index);

  // nothing to do, return empty string
  if(operation.keySize() == 0) {
    item = "";
    operation.finalize(0);
    finalize(tx, index);
    return rocksdb::Status::NotFound();
  }

  KeyDescriptor &descriptor = operation.descriptor();
  uint64_t listIndex = descriptor.getListIndex(direction);
  uint64_t victim = listIndex + (int)direction*(-1);

  std::string field = unsignedIntToBinaryString(victim);
  qdb_assert(operation.getField(field, item));
  qdb_assert(operation.deleteField(field));
  descriptor.setListIndex(direction, victim);

  operation.finalize(operation.keySize() - 1);
  return finalize(tx, index);
}

rocksdb::Status StateMachine::lpop(const std::string &key, std::string &item, LogIndex index) {
  return listPop(Direction::kLeft, key, item, index);
}

rocksdb::Status StateMachine::rpop(const std::string &key, std::string &item, LogIndex index) {
  return listPop(Direction::kRight, key, item, index);
}

StateMachine::Snapshot::Snapshot(rocksdb::DB *db_) {
  db = db_;
  snapshot = db->GetSnapshot();
  if(snapshot == nullptr) qdb_throw("unable to take db snapshot");
  options.snapshot = snapshot;
}

StateMachine::Snapshot::~Snapshot() {
  db->ReleaseSnapshot(snapshot);
}

rocksdb::ReadOptions& StateMachine::Snapshot::opts() {
  return options;
}

rocksdb::Status StateMachine::get(const std::string &key, std::string &value) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kString)) return wrong_type();

  std::string tkey = translate_key(KeyType::kString, key);
  return db->Get(snapshot.opts(), tkey, &value);
}

void StateMachine::remove_all_with_prefix(const std::string &prefix, int64_t &removed, TransactionPtr &tx) {
  removed = 0;

  std::string tmp;
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));

  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    if(!startswith(key, prefix)) break;
    if(key.size() > 0 && (key[0] == char(InternalKeyType::kInternal) || key[0] == char(KeyType::kConfiguration))) continue;

    THROW_ON_ERROR(tx->Delete(key));
    removed++;
  }
}

rocksdb::Status StateMachine::del(const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  removed = 0;
  TransactionPtr tx = startTransaction();

  for(VecIterator it = start; it != end; it++) {
    KeyDescriptor keyInfo = lockKeyDescriptor(tx, *it);
    if(!keyInfo.exists()) continue;

    std::string tkey, tmp;

    if(keyInfo.type() == KeyType::kString) {
      tkey = translate_key(KeyType::kString, *it);
      THROW_ON_ERROR(tx->GetForUpdate(rocksdb::ReadOptions(), tkey, &tmp));
      THROW_ON_ERROR(tx->Delete(tkey));
    }
    else if(keyInfo.type() == KeyType::kHash || keyInfo.type() == KeyType::kSet || keyInfo.type() == KeyType::kList) {
      tkey = translate_key(keyInfo.type(), *it) + "#";
      int64_t count = 0;
      remove_all_with_prefix(tkey, count, tx);
      if(count != keyInfo.size()) qdb_throw("mismatch between keyInfo counter and number of elements deleted by remove_all_with_prefix: " << count << " vs " << keyInfo.size());
    }
    else {
      qdb_throw("should never happen");
    }

    removed++;
    THROW_ON_ERROR(tx->Delete(keyInfo.getRawKey()));
  }

  return finalize(tx, index);
}

rocksdb::Status StateMachine::exists(const std::string &key) {
  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(keyinfo.exists()) return rocksdb::Status::OK();
  return rocksdb::Status::NotFound();
}

rocksdb::Status StateMachine::keys(const std::string &pattern, std::vector<std::string> &result) {
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

rocksdb::Status StateMachine::flushall(LogIndex index) {
  int64_t tmp;
  TransactionPtr tx = startTransaction();
  remove_all_with_prefix("", tmp, tx);
  return finalize(tx, index);
}

rocksdb::Status StateMachine::checkpoint(const std::string &path) {
  rocksdb::Checkpoint *checkpoint = nullptr;
  RETURN_ON_ERROR(rocksdb::Checkpoint::Create(db, &checkpoint));

  rocksdb::Status st = checkpoint->CreateCheckpoint(path);
  delete checkpoint;

  return st;
}

std::string StateMachine::statistics() {
  std::string stats;
  db->GetProperty("rocksdb.stats", &stats);
  return stats;
}

StateMachine::TransactionPtr StateMachine::startTransaction() {
  rocksdb::WriteOptions opts;
  opts.disableWAL = !writeAheadLog;
  return TransactionPtr(transactionDB->BeginTransaction(opts));
}

rocksdb::Status StateMachine::noop(LogIndex index) {
  TransactionPtr tx = startTransaction();
  return finalize(tx, index);
}

rocksdb::Status StateMachine::finalize(TransactionPtr &tx, LogIndex index) {
  commitTransaction(tx, index);
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::malformedRequest(TransactionPtr &tx, LogIndex index, std::string message) {
  commitTransaction(tx, index);
  return rocksdb::Status::InvalidArgument(message);
}

void StateMachine::commitTransaction(TransactionPtr &tx, LogIndex index) {
  if(index <= 0 && lastApplied > 0) qdb_throw("provided invalid index for version-tracked database: " << index << ", current last applied: " << lastApplied);

  if(index > 0) {
    if(index != lastApplied+1) qdb_throw("attempted to perform illegal lastApplied update: " << lastApplied << " ==> " << index);
    THROW_ON_ERROR(tx->Put("__last-applied", intToBinaryString(index)));
  }

  rocksdb::Status st = tx->Commit();
  if(index > 0 && st.ok()) lastApplied = index;

  if(!st.ok()) qdb_throw("unable to commit transaction with index " << index << ": " << st.ToString());
}
