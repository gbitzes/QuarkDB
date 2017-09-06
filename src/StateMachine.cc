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
#include "../deps/StringMatchLen.h"
#include "storage/KeyDescriptor.hh"
#include "storage/KeyLocators.hh"
#include "storage/StagingArea.hh"
#include "storage/KeyDescriptorBuilder.hh"
#include "utils/IntToBinaryString.hh"
#include <sys/stat.h>
#include <rocksdb/status.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <thread>

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

static rocksdb::Status malformed(const std::string &message) {
  return rocksdb::Status::InvalidArgument(message);
}

StateMachine::StateMachine(const std::string &f, bool write_ahead_log, bool bulk_load)
: filename(f), writeAheadLog(write_ahead_log), bulkLoad(bulk_load) {

  if(writeAheadLog) {
    qdb_info("Openning state machine " << quotes(filename) << ".");
  }
  else {
    qdb_warn("Opening state machine " << quotes(filename) << " *without* write ahead log - an unclean shutdown WILL CAUSE DATA LOSS");
  }

  bool dirExists = directoryExists(filename);

  if(bulkLoad && dirExists) {
    qdb_throw("bulkload only available for newly initialized state machines; path '" << filename << "' already exists");
  }

  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(32, false));
  table_options.block_size = 16 * 1024;

  // The default settings for rate limiting are a bit too conservative, causing
  // bulk loading to stall heavily.
  options.max_write_buffer_number = 6;
  options.soft_pending_compaction_bytes_limit = 256 * 1073741824ull;
  options.hard_pending_compaction_bytes_limit = 512 * 1073741824ull;
  options.level0_slowdown_writes_trigger = 50;
  options.level0_stop_writes_trigger = 75;

  options.create_if_missing = !dirExists;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

  if(bulkLoad) {
    qdb_warn("Opening state machine in bulkload mode.");
    writeAheadLog = false;
    options.max_background_jobs = std::max(4u, std::thread::hardware_concurrency());
    options.PrepareForBulkLoad();
    options.memtable_factory.reset(new rocksdb::VectorRepFactory());
    options.allow_concurrent_memtable_write = false;
  }

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

static std::string translate_key(const InternalKeyType type, const std::string &key) {
  return std::string(1, char(type)) + key;
}

static rocksdb::Status wrong_type() {
  return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
}

static KeyDescriptor constructDescriptor(rocksdb::Status &st, const std::string &serialization) {
  if(st.IsNotFound()) {
    return KeyDescriptor();
  }

  if(!st.ok()) qdb_throw("unexpected rocksdb status when inspecting key descriptor");
  return KeyDescriptor(serialization);
}

KeyDescriptor StateMachine::getKeyDescriptor(const std::string &redisKey) {
  std::string tmp;
  DescriptorLocator dlocator(redisKey);
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), dlocator.toSlice(), &tmp);
  return constructDescriptor(st, tmp);
}

KeyDescriptor StateMachine::getKeyDescriptor(StateMachine::Snapshot &snapshot, const std::string &redisKey) {
  std::string tmp;
  DescriptorLocator dlocator(redisKey);
  rocksdb::Status st = db->Get(snapshot.opts(), dlocator.toSlice(), &tmp);
  return constructDescriptor(st, tmp);
}

KeyDescriptor StateMachine::lockKeyDescriptor(StagingArea &stagingArea, DescriptorLocator &dlocator) {
  std::string tmp;
  rocksdb::Status st = stagingArea.getForUpdate(dlocator.toSlice(), tmp);
  return constructDescriptor(st, tmp);
}

bool StateMachine::assertKeyType(Snapshot &snapshot, const std::string &key, KeyType keytype) {
  KeyDescriptor keyinfo = getKeyDescriptor(snapshot, key);
  if(!keyinfo.empty() && keyinfo.getKeyType() != keytype) return false;
  return true;
}

rocksdb::Status StateMachine::hget(const std::string &key, const std::string &field, std::string &value) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  FieldLocator locator(KeyType::kHash, key, field);
  return db->Get(snapshot.opts(), locator.toSlice(), &value);
}

rocksdb::Status StateMachine::hexists(const std::string &key, const std::string &field) {
  std::string tmp;
  return this->hget(key, field, tmp);
}

rocksdb::Status StateMachine::hkeys(const std::string &key, std::vector<std::string> &keys) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  keys.clear();
  FieldLocator locator(KeyType::kHash, key);

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startswith(tmp, locator.toSlice())) break;
    keys.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hgetall(const std::string &key, std::vector<std::string> &res) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  res.clear();
  FieldLocator locator(KeyType::kHash, key);

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startswith(tmp, locator.toSlice())) break;
    res.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
    res.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}


rocksdb::Status StateMachine::hset(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated) {
  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  operation.writeField(field, value);
  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::hmset(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end) {
  if((end - start) % 2 != 0) qdb_throw("hmset: distance between start and end iterators must be an even number");

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  int64_t newsize = operation.keySize();
  for(VecIterator it = start; it != end; it += 2) {
    newsize += !operation.fieldExists(*it);
    operation.writeField(*it, *(it+1));
  }

  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::hsetnx(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated) {
  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  if(fieldcreated) {
    operation.writeField(field, value);
  }

  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::hincrby(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  int64_t incrbyInt64;
  if(!my_strtoll(incrby, incrbyInt64)) {
    return malformed("value is not an integer or out of range");
  }

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  std::string value;
  bool exists = operation.getField(field, value);

  result = 0;
  if(exists && !my_strtoll(value, result)) {
    operation.finalize(operation.keySize());
    return malformed("hash value is not an integer");
  }

  result += incrbyInt64;

  operation.writeField(field, std::to_string(result));
  return operation.finalize(operation.keySize() + !exists);
}

rocksdb::Status StateMachine::hincrbyfloat(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &incrby, double &result) {
  double incrByDouble;
  if(!my_strtod(incrby, incrByDouble)) {
    return malformed("value is not a float or out of range");
  }

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  std::string value;
  bool exists = operation.getField(field, value);

  result = 0;
  if(exists && !my_strtod(value, result)) {
    operation.finalize(operation.keySize());
    return malformed("hash value is not a float");
  }

  result += incrByDouble;

  operation.writeField(field, std::to_string(result));
  return operation.finalize(operation.keySize() + !exists);
}

rocksdb::Status StateMachine::hdel(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed) {
  removed = 0;

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  for(VecIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  int64_t newsize = operation.keySize() - removed;
  return operation.finalize(newsize);
}


static bool isWrongType(KeyDescriptor &descriptor, KeyType keyType) {
  return !descriptor.empty() && (descriptor.getKeyType() != keyType);
}

rocksdb::Status StateMachine::hlen(const std::string &key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(isWrongType(keyinfo, KeyType::kHash)) return wrong_type();

  len = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hscan(const std::string &key, const std::string &cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kHash)) return wrong_type();

  FieldLocator locator(KeyType::kHash, key, cursor);
  res.clear();

  newCursor = "";
  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(locator.toSlice()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();

    if(!StringUtils::startswith(tmp, locator.getPrefix())) break;

    std::string fieldname = std::string(tmp.begin()+locator.getPrefixSize(), tmp.end());
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

  FieldLocator locator(KeyType::kHash, key);
  vals.clear();

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startswith(tmp, locator.toSlice())) break;
    vals.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::sadd(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &added) {
  added = 0;

  WriteOperation operation(stagingArea, key, KeyType::kSet);
  if(!operation.valid()) return wrong_type();

  for(VecIterator it = start; it != end; it++) {
    bool exists = operation.fieldExists(*it);
    if(!exists) {
      operation.writeField(*it, "1");
      added++;
    }
  }

  return operation.finalize(operation.keySize() + added);
}

rocksdb::Status StateMachine::sismember(const std::string &key, const std::string &element) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kSet)) return wrong_type();
  FieldLocator locator(KeyType::kSet, key, element);

  std::string tmp;
  return db->Get(snapshot.opts(), locator.toSlice(), &tmp);
}

rocksdb::Status StateMachine::srem(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed) {
  removed = 0;

  WriteOperation operation(stagingArea, key, KeyType::kSet);
  if(!operation.valid()) return wrong_type();

  for(VecIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  return operation.finalize(operation.keySize() - removed);
}

rocksdb::Status StateMachine::smembers(const std::string &key, std::vector<std::string> &members) {
  Snapshot snapshot(db);
  if(!assertKeyType(snapshot, key, KeyType::kSet)) return wrong_type();

  FieldLocator locator(KeyType::kSet, key);
  members.clear();

  IteratorPtr iter(db->NewIterator(snapshot.opts()));
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startswith(tmp, locator.toSlice())) break;
    members.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::scard(const std::string &key, size_t &count) {
  count = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(isWrongType(keyinfo, KeyType::kSet)) return wrong_type();

  count = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::configGet(const std::string &key, std::string &value) {
  Snapshot snapshot(db);
  std::string tkey = translate_key(InternalKeyType::kConfiguration, key);
  return db->Get(snapshot.opts(), tkey, &value);
}

rocksdb::Status StateMachine::configSet(StagingArea &stagingArea, const std::string &key, const std::string &value) {
  // We don't use WriteOperation or key descriptors here,
  // since kConfiguration is special.

  std::string tkey = translate_key(InternalKeyType::kConfiguration, key);
  stagingArea.put(tkey, value);
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::configGetall(std::vector<std::string> &res) {
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  res.clear();

  std::string searchPrefix(1, char(InternalKeyType::kConfiguration));
  for(iter->Seek(searchPrefix); iter->Valid(); iter->Next()) {
    std::string rkey = iter->key().ToString();
    if(rkey.size() == 0 || rkey[0] != char(InternalKeyType::kConfiguration)) break;

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

StateMachine::WriteOperation::WriteOperation(StagingArea &staging, const std::string &key, const KeyType &type)
: stagingArea(staging), redisKey(key), expectedType(type) {

  std::string tmp;

  dlocator.reset(redisKey);
  rocksdb::Status st = stagingArea.getForUpdate(dlocator.toSlice(), tmp);

  if(st.IsNotFound()) {
    keyinfo = KeyDescriptor();
  }
  else if(st.ok()) {
    keyinfo = KeyDescriptor(tmp);
  }
  else {
    qdb_throw("unexpected rocksdb status when inspecting KeyType entry " << dlocator.toString() << ": " << st.ToString());
  }

  redisKeyExists = !keyinfo.empty();
  isValid = (keyinfo.empty()) || (keyinfo.getKeyType() == type);

  if(keyinfo.empty() && isValid) {
    keyinfo.setKeyType(expectedType);
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

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  rocksdb::Status st = stagingArea.get(locator.toSlice(), out);
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

int64_t StateMachine::WriteOperation::keySize() {
  return keyinfo.getSize();
}

void StateMachine::WriteOperation::assertWritable() {
  if(!isValid) qdb_throw("WriteOperation not valid!");
  if(finalized) qdb_throw("WriteOperation already finalized!");
}

void StateMachine::WriteOperation::write(const std::string &value) {
  assertWritable();

  if(keyinfo.getKeyType() != KeyType::kString) {
    qdb_throw("writing without a field makes sense only for strings");
  }

  StringLocator slocator(redisKey);
  stagingArea.put(slocator.toSlice(), value);
}

void StateMachine::WriteOperation::writeField(const std::string &field, const std::string &value) {
  assertWritable();

  if(keyinfo.getKeyType() != KeyType::kHash && keyinfo.getKeyType() != KeyType::kSet && keyinfo.getKeyType() != KeyType::kList) {
    qdb_throw("writing with a field makes sense only for hashes, sets, or lists");
  }

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  stagingArea.put(locator.toSlice(), value);
}

rocksdb::Status StateMachine::WriteOperation::finalize(int64_t newsize) {
  assertWritable();

  if(newsize < 0) qdb_throw("invalid newsize: " << newsize);

  if(newsize == 0) {
    stagingArea.del(dlocator.toSlice());
  }
  else if(keyinfo.getSize() != newsize) {
    keyinfo.setSize(newsize);
    stagingArea.put(dlocator.toSlice(), keyinfo.serialize());
  }

  finalized = true;
  return rocksdb::Status::OK(); // OK if return value is ignored
}

bool StateMachine::WriteOperation::fieldExists(const std::string &field) {
  assertWritable();

  std::string tmp;
  return getField(field, tmp);
}

bool StateMachine::WriteOperation::deleteField(const std::string &field) {
  assertWritable();

  std::string tmp;

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  rocksdb::Status st = stagingArea.get(locator.toSlice(), tmp);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) stagingArea.del(locator.toSlice());
  return st.ok();
}

rocksdb::Status StateMachine::set(StagingArea &stagingArea, const std::string& key, const std::string& value) {
  WriteOperation operation(stagingArea, key, KeyType::kString);
  if(!operation.valid()) return wrong_type();

  operation.write(value);
  return operation.finalize(value.size());
}

rocksdb::Status StateMachine::listPush(StagingArea &stagingArea, Direction direction, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length) {
  WriteOperation operation(stagingArea, key, KeyType::kList);
  if(!operation.valid()) return wrong_type();

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
  return operation.finalize(length);
}

rocksdb::Status StateMachine::lpush(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length) {
  return this->listPush(stagingArea, Direction::kLeft, key, start, end, length);
}

rocksdb::Status StateMachine::rpush(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length) {
  return this->listPush(stagingArea, Direction::kRight, key, start, end, length);
}

rocksdb::Status StateMachine::lpop(StagingArea &stagingArea, const std::string &key, std::string &item) {
  return this->listPop(stagingArea, Direction::kLeft, key, item);
}

rocksdb::Status StateMachine::rpop(StagingArea &stagingArea, const std::string &key, std::string &item) {
  return this->listPop(stagingArea, Direction::kRight, key, item);
}

rocksdb::Status StateMachine::llen(const std::string &key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(key);
  if(isWrongType(keyinfo, KeyType::kList)) return wrong_type();

  len = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::listPop(StagingArea &stagingArea, Direction direction, const std::string &key, std::string &item) {
  WriteOperation operation(stagingArea, key, KeyType::kList);
  if(!operation.valid()) return wrong_type();

  // nothing to do, return empty string
  if(operation.keySize() == 0) {
    item = "";
    operation.finalize(0);
    return rocksdb::Status::NotFound();
  }

  KeyDescriptor &descriptor = operation.descriptor();
  uint64_t listIndex = descriptor.getListIndex(direction);
  uint64_t victim = listIndex + (int)direction*(-1);

  std::string field = unsignedIntToBinaryString(victim);
  qdb_assert(operation.getField(field, item));
  qdb_assert(operation.deleteField(field));
  descriptor.setListIndex(direction, victim);

  return operation.finalize(operation.keySize() - 1);
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

  StringLocator slocator(key);
  return db->Get(snapshot.opts(), slocator.toSlice(), &value);
}

void StateMachine::remove_all_with_prefix(const rocksdb::Slice &prefix, int64_t &removed, StagingArea &stagingArea) {
  removed = 0;

  std::string tmp;
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));

  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    if(!StringUtils::startswith(key, prefix)) break;
    if(key.size() > 0 && (key[0] == char(InternalKeyType::kInternal) || key[0] == char(InternalKeyType::kConfiguration))) continue;

    stagingArea.del(key);
    removed++;
  }
}

rocksdb::Status StateMachine::del(StagingArea &stagingArea, const VecIterator &start, const VecIterator &end, int64_t &removed) {
  removed = 0;

  for(VecIterator it = start; it != end; it++) {
    DescriptorLocator dlocator(*it);
    KeyDescriptor keyInfo = lockKeyDescriptor(stagingArea, dlocator);
    if(keyInfo.empty()) continue;

    std::string tmp;

    if(keyInfo.getKeyType() == KeyType::kString) {
      StringLocator slocator(*it);
      THROW_ON_ERROR(stagingArea.get(slocator.toSlice(), tmp));
      stagingArea.del(slocator.toSlice());
    }
    else if(keyInfo.getKeyType() == KeyType::kHash || keyInfo.getKeyType() == KeyType::kSet || keyInfo.getKeyType() == KeyType::kList) {
      FieldLocator locator(keyInfo.getKeyType(), *it);
      int64_t count = 0;
      remove_all_with_prefix(locator.toSlice(), count, stagingArea);
      if(count != keyInfo.getSize()) qdb_throw("mismatch between keyInfo counter and number of elements deleted by remove_all_with_prefix: " << count << " vs " << keyInfo.getSize());
    }
    else {
      qdb_throw("should never happen");
    }

    removed++;
    stagingArea.del(dlocator.toSlice());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::exists(const VecIterator &start, const VecIterator &end, int64_t &count) {
  count = 0;
  Snapshot snapshot(db);

  for(auto it = start; it != end; it++) {
    KeyDescriptor keyinfo = getKeyDescriptor(snapshot, *it);

    if(!keyinfo.empty()) {
      count++;
    }
  }

  return rocksdb::Status::OK();
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

rocksdb::Status StateMachine::flushall(StagingArea &stagingArea) {
  int64_t tmp;
  remove_all_with_prefix("", tmp, stagingArea);
  return rocksdb::Status::OK();
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
  StagingArea stagingArea(*this);
  return stagingArea.commit(index);
}

void StateMachine::finalizeBulkload() {
  qdb_event("Finalizing bulkload, issuing manual compaction...");
  THROW_ON_ERROR(db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr));
  qdb_event("Manual compaction was successful. Building key descriptors...");
  KeyDescriptorBuilder builder(*this);
  qdb_event("All done, bulk load is over. Restart quarkdb in standalone mode.");

  // auto it = descriptorCache.begin();
  // rocksdb::WriteBatch descriptors;
  //
  // size_t count = 0;
  // while(it != descriptorCache.end()) {
  //   count++;
  //   descriptors.Put(it->first, it->second->value);
  //
  //   it++;
  // }
  //
  // qdb_event("Collected " << count << " key descriptors. Flushing write batch...");
  // commitBatch(descriptors);
  //
  // qdb_event("Key descriptors have been flushed. Issuing manual compaction...");
  // THROW_ON_ERROR(db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr));
  // qdb_event("Compaction has been successful - bulk load is over. Restart quarkdb in standalone mode.");
}

StateMachine::IteratorPtr StateMachine::getRawIterator() {
  rocksdb::ReadOptions readOpts;
  readOpts.total_order_seek = true;
  return IteratorPtr(db->NewIterator(readOpts));
}

void StateMachine::commitBatch(rocksdb::WriteBatch &batch) {
  rocksdb::WriteOptions opts;
  opts.disableWAL = !writeAheadLog;
  THROW_ON_ERROR(db->Write(opts, &batch));
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

// Simple API for writes - chain to pipelined API using a single operation per batch

#define CHAIN(index, func, ...) { \
  StagingArea stagingArea(*this); \
  rocksdb::Status st = this->func(stagingArea, ## __VA_ARGS__); \
  stagingArea.commit(index); \
  return st; \
}

rocksdb::Status StateMachine::hset(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index) {
  CHAIN(index, hset, key, field, value, fieldcreated);
}

rocksdb::Status StateMachine::hmset(const std::string &key, const VecIterator &start, const VecIterator &end, LogIndex index) {
  CHAIN(index, hmset, key, start, end);
}

rocksdb::Status StateMachine::hsetnx(const std::string &key, const std::string &field, const std::string &value, bool &fieldcreated, LogIndex index) {
  CHAIN(index, hsetnx, key, field, value, fieldcreated);
}

rocksdb::Status StateMachine::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result, LogIndex index) {
  CHAIN(index, hincrby, key, field, incrby, result);
}

rocksdb::Status StateMachine::hincrbyfloat(const std::string &key, const std::string &field, const std::string &incrby, double &result, LogIndex index) {
  CHAIN(index, hincrbyfloat, key, field, incrby, result);
}

rocksdb::Status StateMachine::hdel(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  CHAIN(index, hdel, key, start, end, removed);
}

rocksdb::Status StateMachine::sadd(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &added, LogIndex index) {
  CHAIN(index, sadd, key, start, end, added);
}

rocksdb::Status StateMachine::srem(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  CHAIN(index, srem, key, start, end, removed);
}

rocksdb::Status StateMachine::set(const std::string& key, const std::string& value, LogIndex index) {
  CHAIN(index, set, key, value);
}

rocksdb::Status StateMachine::del(const VecIterator &start, const VecIterator &end, int64_t &removed, LogIndex index) {
  CHAIN(index, del, start, end, removed);
}

rocksdb::Status StateMachine::flushall(LogIndex index) {
  CHAIN(index, flushall);
}

rocksdb::Status StateMachine::lpop(const std::string &key, std::string &item, LogIndex index) {
  CHAIN(index, lpop, key, item);
}

rocksdb::Status StateMachine::rpop(const std::string &key, std::string &item, LogIndex index) {
  CHAIN(index, rpop, key, item);
}

rocksdb::Status StateMachine::lpush(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index) {
  CHAIN(index, lpush, key, start, end, length);
}

rocksdb::Status StateMachine::rpush(const std::string &key, const VecIterator &start, const VecIterator &end, int64_t &length, LogIndex index) {
  CHAIN(index, rpush, key, start, end, length);
}

rocksdb::Status StateMachine::configSet(const std::string &key, const std::string &value, LogIndex index) {
  CHAIN(index, configSet, key, value);
}
