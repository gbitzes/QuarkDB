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
#include "utils/ParseUtils.hh"
#include "storage/KeyDescriptor.hh"
#include "storage/KeyLocators.hh"
#include "storage/StagingArea.hh"
#include "storage/KeyDescriptorBuilder.hh"
#include "storage/PatternMatching.hh"
#include "storage/ExpirationEventIterator.hh"
#include "storage/ReverseLocator.hh"
#include "utils/IntToBinaryString.hh"
#include "utils/TimeFormatting.hh"
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

static rocksdb::Status malformed(std::string_view message) {
  return rocksdb::Status::InvalidArgument(message);
}

StateMachine::StateMachine(std::string_view f, bool write_ahead_log, bool bulk_load)
: filename(f), writeAheadLog(write_ahead_log), bulkLoad(bulk_load), timeKeeper(0u),
 requestCounter(std::chrono::seconds(10)) {

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
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.block_size = 16 * 1024;

  // This option prevents creating bloom filters for the last compaction level.
  // A bloom filter is used to quickly rule out whether an SST may contain a
  // given key or not. Having bloom filters for the last compaction layer is
  // not particularly useful, as it only prevents an extra IO read in cases
  // where a key is not found. Given that the last compaction layer is the
  // biggest, turning on this option reduces total bloom filter size on disk
  // (and associated memory consumption) by ~90%, while only making "not-found"
  // queries slightly more expensive.
  options.optimize_filters_for_hits = true;

  // The default settings for rate limiting are a bit too conservative, causing
  // bulk loading to stall heavily.
  options.max_write_buffer_number = 6;
  options.soft_pending_compaction_bytes_limit = 256 * 1073741824ull;
  options.hard_pending_compaction_bytes_limit = 512 * 1073741824ull;
  options.level0_slowdown_writes_trigger = 50;
  options.level0_stop_writes_trigger = 75;

  // rocksdb replays the MANIFEST file upon startup to detect possible DB
  // corruption. This file grows by the number of SST files updated per run,
  // and is reset after each run.
  // If the DB runs for too long, accumulating too many updates, the next
  // restart will potentially take several minutes.
  // This option limits the max size of MANIFEST to 2MB, taking care to
  // automatically roll-over when necessary, which should alleviate the above.

  if(!bulkLoad) {
    options.max_manifest_file_size = 1024 * 1024;
  }

  options.compression = rocksdb::kLZ4Compression;
  options.bottommost_compression = rocksdb::kZSTD;

  options.create_if_missing = !dirExists;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
  options.row_cache = rocksdb::NewLRUCache(1024 * 1024 * 1024, 8);

  // Use multiple threads for compaction and flushing jobs
  options.IncreaseParallelism(std::max(2u, std::thread::hardware_concurrency() / 2));

  // Parallelize compaction, but limit maximum number of subcompactions to 4.
  options.max_subcompactions = std::max(1u, std::thread::hardware_concurrency() / 2);
  if(options.max_subcompactions > 4) {
    options.max_subcompactions = 4;
  }

  // Let rocksdb itself decide the target sizes for each compaction level
  options.level_compaction_dynamic_level_bytes = true;
  options.disable_auto_compactions = false;

  if(bulkLoad) {
    qdb_warn("Opening state machine in bulkload mode.");
    writeAheadLog = false;
    options.PrepareForBulkLoad();
    options.memtable_factory.reset(new rocksdb::VectorRepFactory());
    options.allow_concurrent_memtable_write = false;
  }

  rocksdb::DB *tmpdb = nullptr;
  rocksdb::Status status = rocksdb::DB::Open(options, filename, &tmpdb);
  if(!status.ok()) qdb_throw("Cannot open " << quotes(filename) << ":" << status.ToString());

  db.reset(tmpdb);
  ensureCompatibleFormat(!dirExists);
  ensureBulkloadSanity(!dirExists);
  ensureClockSanity(!dirExists);
  retrieveLastApplied();

  consistencyScanner.reset(new ConsistencyScanner(*this));
}

void StateMachine::ensureClockSanity(bool justCreated) {
  std::string value;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), KeyConstants::kStateMachine_Clock, &value);

  if(justCreated) {
    if(!st.IsNotFound()) qdb_throw("Error when reading __clock, which should not exist: " << st.ToString());
    THROW_ON_ERROR(db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_Clock, unsignedIntToBinaryString(0u)));
  }
  else {
    if(st.IsNotFound()) {
      // Compatibility: When opening old state machines, set expected __clock key.
      // TODO: Remove in a couple of releases.
      THROW_ON_ERROR(db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_Clock, unsignedIntToBinaryString(0u)));
    }
  }

  st = db->Get(rocksdb::ReadOptions(), KeyConstants::kStateMachine_Clock, &value);
  if(!st.ok()) qdb_throw("Error when reading __clock: " << st.ToString());

  if(value.size() != 8u) {
    qdb_throw("Detected corruption of __clock, received size " << value.size() << ", was expecting 8");
  }

  // We survived!
  timeKeeper.reset(binaryStringToUnsignedInt(value.c_str()));
}

StateMachine::~StateMachine() {
  consistencyScanner.reset();

  if(db) {
    qdb_info("Closing state machine " << quotes(filename));
    db.reset();
  }
}

//------------------------------------------------------------------------------
// Get underlying folder where this SM resides
//------------------------------------------------------------------------------
std::string StateMachine::getPhysicalLocation() const {
  return filename;
}

void StateMachine::reset() {
  IteratorPtr iter(db->NewIterator(rocksdb::ReadOptions()));
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    db->Delete(rocksdb::WriteOptions(), iter->key().ToString());
  }

  ensureCompatibleFormat(true);
  ensureBulkloadSanity(true);
  ensureClockSanity(true);
  retrieveLastApplied();
}

void StateMachine::hardSynchronizeDynamicClock() {
  ClockValue syncPoint;
  getClock(syncPoint);

  timeKeeper.synchronize(syncPoint);
}

ClockValue StateMachine::getDynamicClock() {
  return timeKeeper.getDynamicTime();
}

void StateMachine::ensureBulkloadSanity(bool justCreated) {
  std::string inBulkload;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), KeyConstants::kStateMachine_InBulkload, &inBulkload);

  if(justCreated) {
    if(!st.IsNotFound()) qdb_throw("Error when reading __in-bulkload, which should not exist: " << st.ToString());
    THROW_ON_ERROR(db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_InBulkload, boolToString(bulkLoad)));
  }
  else {
    if(st.IsNotFound()) {
      // Compatibility: When opening old state machines, set expected __in-bulkload key.
      // TODO: Remove once PPS machines have been updated..
      THROW_ON_ERROR(db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_InBulkload, boolToString(false)));
      st = db->Get(rocksdb::ReadOptions(), KeyConstants::kStateMachine_InBulkload, &inBulkload);
    }

    if(!st.ok()) qdb_throw("Error when reading __in-bulkload: " << st.ToString());
    if(inBulkload != boolToString(false)) qdb_throw("Bulkload mode was NOT finalized! DB is corrupted - you either did not call finalizeBulkload, or you copied live SST files without shutting down the bulkload QDB process first." << st.ToString());
  }
}

void StateMachine::ensureCompatibleFormat(bool justCreated) {
  const std::string currentFormat("0");

  std::string format;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), KeyConstants::kStateMachine_Format, &format);

  if(justCreated) {
    if(!st.IsNotFound()) qdb_throw("Error when reading __format, which should not exist: " << st.ToString());

    st = db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_Format, currentFormat);
    if(!st.ok()) qdb_throw("error when setting format: " << st.ToString());
  }
  else {
    if(!st.ok()) qdb_throw("Cannot read __format: " << st.ToString());
    if(format != currentFormat) qdb_throw("Asked to open a state machine with incompatible format (" << format << "), I can only handle " << currentFormat);
  }
}

void StateMachine::retrieveLastApplied() {
  std::string tmp;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), KeyConstants::kStateMachine_LastApplied, &tmp);

  if(st.ok()) {
    lastApplied = binaryStringToInt(tmp.c_str());
  }
  else if(st.IsNotFound()) {
    lastApplied = 0;
    st = db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_LastApplied, intToBinaryString(lastApplied));
    if(!st.ok()) qdb_throw("error when setting lastApplied: " << st.ToString());
  }
  else {
    qdb_throw("error when retrieving lastApplied: " << st.ToString());
  }
}

LogIndex StateMachine::getLastApplied() {
  return lastApplied;
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

KeyDescriptor StateMachine::getKeyDescriptor(StagingArea &stagingArea, std::string_view redisKey) {
  std::string tmp;
  DescriptorLocator dlocator(redisKey);
  rocksdb::Status st = stagingArea.get(dlocator.toView(), tmp);
  return constructDescriptor(st, tmp);
}

KeyDescriptor StateMachine::lockKeyDescriptor(StagingArea &stagingArea, DescriptorLocator &dlocator) {
  std::string tmp;
  rocksdb::Status st = stagingArea.getForUpdate(dlocator.toView(), tmp);
  return constructDescriptor(st, tmp);
}

bool StateMachine::assertKeyType(StagingArea &stagingArea, std::string_view key, KeyType keytype) {
  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(!keyinfo.empty() && keyinfo.getKeyType() != keytype) return false;
  return true;
}

rocksdb::Status StateMachine::hget(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string &value) {
  if(!assertKeyType(stagingArea, key, KeyType::kHash)) return wrong_type();

  FieldLocator locator(KeyType::kHash, key, field);
  return stagingArea.get(locator.toView(), value);
}

rocksdb::Status StateMachine::hexists(StagingArea &stagingArea, std::string_view key, std::string_view field) {
  std::string tmp;
  return this->hget(stagingArea, key, field, tmp);
}

rocksdb::Status StateMachine::hkeys(StagingArea &stagingArea, std::string_view key, std::vector<std::string> &keys) {
  if(!assertKeyType(stagingArea, key, KeyType::kHash)) return wrong_type();

  keys.clear();
  FieldLocator locator(KeyType::kHash, key);

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startsWith(tmp, locator.toView())) break;
    keys.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hgetall(StagingArea &stagingArea, std::string_view key, std::vector<std::string> &res) {
  if(!assertKeyType(stagingArea, key, KeyType::kHash)) return wrong_type();

  res.clear();
  FieldLocator locator(KeyType::kHash, key);

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startsWith(tmp, locator.toView())) break;
    res.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
    res.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

void StateMachine::lhsetInternal(WriteOperation &operation, std::string_view key, std::string_view field, std::string_view hint, std::string_view value, bool &fieldcreated) {
  fieldcreated = false;

  if(operation.localityFieldExists(hint, field)) {
    // Cool, field exists, we take the fast path. Just update a single value,
    // and we are done. No need to update any indexes or key descriptor size,
    // as we simply override the old value.
    operation.writeLocalityField(hint, field, value);
    return;
  }

  // Two cases: We've received a different locality hint, or we're creating
  // a new field.

  std::string previousHint;
  if(operation.getLocalityIndex(field, previousHint)) {
    // Changing locality hint. Drop old entry, insert new one.
    qdb_assert(operation.deleteLocalityField(previousHint, field));

    // Update field and index.
    operation.writeLocalityField(hint, field, value);
    operation.writeLocalityIndex(field, hint);

    // No update on key size, we're just rewriting a key.
    return;
  }

  // New field!
  fieldcreated = true;
  operation.writeLocalityField(hint, field, value);
  operation.writeLocalityIndex(field, hint);
  return;
}

rocksdb::Status StateMachine::lhmset(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end) {
  if((end - start) % 3 != 0) qdb_throw("lhmset: distance between start and end iterators must be a multiple of three");

  WriteOperation operation(stagingArea, key, KeyType::kLocalityHash);
  if(!operation.valid()) return wrong_type();

  int64_t created = 0;
  for(auto it = start; it != end; it += 3) {
    bool fieldcreated = false;
    lhsetInternal(operation, key, *it, *(it+1), *(it+2), fieldcreated);
    created += fieldcreated;
  }

  return operation.finalize(operation.keySize() + created);
}

rocksdb::Status StateMachine::lhset(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view hint, std::string_view value, bool &fieldcreated) {
  WriteOperation operation(stagingArea, key, KeyType::kLocalityHash);
  if(!operation.valid()) return wrong_type();

  fieldcreated = false;
  lhsetInternal(operation, key, field, hint, value, fieldcreated);
  return operation.finalize(operation.keySize() + fieldcreated);
}

rocksdb::Status StateMachine::lhdel(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &removed) {
  removed = 0;

  WriteOperation operation(stagingArea, key, KeyType::kLocalityHash);
  if(!operation.valid()) return wrong_type();

  for(ReqIterator it = start; it != end; it++) {
    std::string hint;
    bool exists = operation.getAndDeleteLocalityIndex(*it, hint);
    if(exists) {
      removed++;
      qdb_assert(operation.deleteLocalityField(hint, *it));
    }
  }

  int64_t newsize = operation.keySize() - removed;
  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::lhget(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view hint, std::string &value) {
  if(!assertKeyType(stagingArea, key, KeyType::kLocalityHash)) return wrong_type();

  if(!hint.empty()) {
    // We were given a hint, whooo. Fast path.
    LocalityFieldLocator locator(key, hint, field);

    rocksdb::Status st = stagingArea.get(locator.toView(), value);
    ASSERT_OK_OR_NOTFOUND(st);

    if(st.ok()) {
      // Done!
      return st;
    }

    // Hmh. Either the field does not exist, or we were given a wrong locality
    // hint.
  }

  std::string correctHint;

  LocalityIndexLocator indexLocator(key, field);
  rocksdb::Status st = stagingArea.get(indexLocator.toView(), correctHint);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.IsNotFound()) return st;

  if(!hint.empty()) {
    // Client is drunk and giving wrong locality hints, warn.
    qdb_assert(hint != correctHint);
    qdb_warn("Received invalid locality hint (" << hint << " vs " << correctHint << ") for locality hash with key " << key << ", targeting field " << field);
  }

  // Fetch correct hint.
  LocalityFieldLocator fieldLocator(key, correctHint, field);
  THROW_ON_ERROR(stagingArea.get(fieldLocator.toView(), value));
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hset(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view value, bool &fieldcreated) {
  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  operation.writeField(field, value);
  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::hmset(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end) {
  if((end - start) % 2 != 0) qdb_throw("hmset: distance between start and end iterators must be an even number");

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  int64_t newsize = operation.keySize();
  for(ReqIterator it = start; it != end; it += 2) {
    newsize += !operation.fieldExists(*it);
    operation.writeField(*it, *(it+1));
  }

  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::hsetnx(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view value, bool &fieldcreated) {
  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;

  if(fieldcreated) {
    operation.writeField(field, value);
  }

  return operation.finalize(newsize);
}

rocksdb::Status StateMachine::hincrby(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view incrby, int64_t &result) {
  int64_t incrbyInt64;
  if(!ParseUtils::parseInt64(incrby, incrbyInt64)) {
    return malformed("value is not an integer or out of range");
  }

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  std::string value;
  bool exists = operation.getField(field, value);

  result = 0;
  if(exists && !ParseUtils::parseInt64(value, result)) {
    operation.finalize(operation.keySize());
    return malformed("hash value is not an integer");
  }

  result += incrbyInt64;

  operation.writeField(field, std::to_string(result));
  return operation.finalize(operation.keySize() + !exists);
}

rocksdb::Status StateMachine::hincrbyfloat(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view incrby, double &result) {
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

rocksdb::Status StateMachine::hdel(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &removed) {
  removed = 0;

  WriteOperation operation(stagingArea, key, KeyType::kHash);
  if(!operation.valid()) return wrong_type();

  for(ReqIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  int64_t newsize = operation.keySize() - removed;
  return operation.finalize(newsize);
}


static bool isWrongType(KeyDescriptor &descriptor, KeyType keyType) {
  return !descriptor.empty() && (descriptor.getKeyType() != keyType);
}

rocksdb::Status StateMachine::hlen(StagingArea &stagingArea, std::string_view key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(isWrongType(keyinfo, KeyType::kHash)) return wrong_type();

  len = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::lhlen(StagingArea &stagingArea, std::string_view key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(isWrongType(keyinfo, KeyType::kLocalityHash)) return wrong_type();

  len = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::rawGetAllVersions(std::string_view key, std::vector<rocksdb::KeyVersion> &versions) {
  return rocksdb::GetAllKeyVersions(db.get(), key, key,
    std::numeric_limits<size_t>::max(), &versions);
}

rocksdb::Status StateMachine::rawScan(StagingArea &stagingArea, std::string_view key, size_t count, std::vector<std::string> &elements) {
  elements.clear();

  IteratorPtr iter(stagingArea.getIterator());

  size_t items = 0;
  for(iter->Seek(key); iter->Valid(); iter->Next()) {
    if(items >= 1000000u || items >= count) break;
    items++;

    elements.emplace_back(iter->key().ToString());
    elements.emplace_back(iter->value().ToString());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hscan(StagingArea &stagingArea, std::string_view key, std::string_view cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
  if(!assertKeyType(stagingArea, key, KeyType::kHash)) return wrong_type();

  FieldLocator locator(KeyType::kHash, key, cursor);
  res.clear();

  newCursor = "";
  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.toView()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();

    if(!StringUtils::startsWith(tmp, locator.getPrefix())) break;

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

rocksdb::Status StateMachine::lhscan(StagingArea &stagingArea, std::string_view key, std::string_view cursor, size_t count, std::string &newCursor, std::vector<std::string> &results) {
  if(!assertKeyType(stagingArea, key, KeyType::kLocalityHash)) return wrong_type();

  std::string_view cursorHint;
  std::string_view cursorField;

  LocalityFieldLocator locator(key);
  results.clear();

  // Any rocksdb keys we touch must have this prefix.
  std::string requiredPrefix(locator.toView());

  if(!cursor.empty()) {
    // Decompose cursor into hint + field.
    EscapedPrefixExtractor extractor;
    if(!extractor.parse(cursor)) {
      return malformed("invalid cursor");
    }

    cursorHint = extractor.getOriginalPrefix();
    cursorField = extractor.getRawSuffix();

    // We start from the given hint + field.
    locator.resetHint(cursorHint);
    locator.resetField(cursorField);
  }

  newCursor = "";
  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.toView()); iter->Valid(); iter->Next()) {
    std::string_view rocksdbKey = iter->key().ToStringView();

    if(!StringUtils::startsWith(rocksdbKey, requiredPrefix)) {
      // It's over, we've iterated through the entire locality hash
      break;
    }

    // Split hint + field
    std::string_view hintPlusField = rocksdbKey;
    hintPlusField.remove_prefix(requiredPrefix.size());

    EscapedPrefixExtractor splitter;
    qdb_assert(splitter.parse(hintPlusField));

    if(results.size() >= count*3) {
      // We've hit result sizelimit, calculate new cursor and break
      newCursor = hintPlusField;
      break;
    }

    // Populate new entry consisting of three items
    results.emplace_back(splitter.getOriginalPrefix());
    results.emplace_back(splitter.getRawSuffix());
    results.emplace_back(iter->value().ToStringView());
  }

  return rocksdb::Status::OK();
}


rocksdb::Status StateMachine::sscan(StagingArea &stagingArea, std::string_view key, std::string_view cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
  if(!assertKeyType(stagingArea, key, KeyType::kSet)) return wrong_type();

  FieldLocator locator(KeyType::kSet, key, cursor);
  res.clear();

  newCursor = "";
  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.toView()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();

    if(!StringUtils::startsWith(tmp, locator.getPrefix())) break;

    std::string fieldname = std::string(tmp.begin()+locator.getPrefixSize(), tmp.end());
    if(res.size() >= count) {
      newCursor = fieldname;
      break;
    }

    res.push_back(fieldname);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::dequeScanBack(StagingArea &stagingArea, std::string_view key, std::string_view cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(isWrongType(keyinfo, KeyType::kDeque)) return wrong_type();

  if(keyinfo.getSize() == 0) {
    newCursor = "0";
    return rocksdb::Status::OK();
  }

  uint64_t cursorMarker;
  if(cursor.size() == 0u) {
    cursorMarker = keyinfo.getEndIndex();
  }
  else if(cursor.size() == 8u) {
    cursorMarker = binaryStringToUnsignedInt(cursor.data());
    if(cursorMarker > keyinfo.getEndIndex()) {
      cursorMarker = keyinfo.getEndIndex();
    }
  }
  else {
    return malformed("invalid cursor");
  }

  uint64_t startingMarker = cursorMarker - count;

  if(startingMarker <= keyinfo.getStartIndex() + 1) {
    newCursor = "0";
    startingMarker = keyinfo.getStartIndex() + 1;
  }
  else {
    newCursor = unsignedIntToBinaryString(startingMarker);
  }

  FieldLocator locator(KeyType::kDeque, key, unsignedIntToBinaryString(startingMarker));

  IteratorPtr iter(stagingArea.getIterator());
  iter->Seek(locator.toView());

  for(uint64_t i = startingMarker; i < cursorMarker; i++) {
    qdb_assert(iter->Valid());

    locator.resetField(unsignedIntToBinaryString(i));
    qdb_assert(locator.toView() == iter->key().ToStringView());
    res.emplace_back(iter->value().ToStringView());

    iter->Next();
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hvals(StagingArea &stagingArea, std::string_view key, std::vector<std::string> &vals) {
  if(!assertKeyType(stagingArea, key, KeyType::kHash)) return wrong_type();

  FieldLocator locator(KeyType::kHash, key);
  vals.clear();

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startsWith(tmp, locator.toView())) break;
    vals.push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::sadd(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &added) {
  added = 0;

  WriteOperation operation(stagingArea, key, KeyType::kSet);
  if(!operation.valid()) return wrong_type();

  for(ReqIterator it = start; it != end; it++) {
    bool exists = operation.fieldExists(*it);
    if(!exists) {
      operation.writeField(*it, "1");
      added++;
    }
  }

  return operation.finalize(operation.keySize() + added);
}

rocksdb::Status StateMachine::sismember(StagingArea &stagingArea, std::string_view key, std::string_view element) {
  if(!assertKeyType(stagingArea, key, KeyType::kSet)) return wrong_type();
  FieldLocator locator(KeyType::kSet, key, element);

  std::string tmp;
  return db->Get(stagingArea.snapshot->opts(), locator.toView(), &tmp);
}

rocksdb::Status StateMachine::srem(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &removed) {
  removed = 0;

  WriteOperation operation(stagingArea, key, KeyType::kSet);
  if(!operation.valid()) return wrong_type();

  for(ReqIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  return operation.finalize(operation.keySize() - removed);
}

rocksdb::Status StateMachine::smove(StagingArea &stagingArea, std::string_view source, std::string_view destination, std::string_view element, int64_t &outcome) {
  WriteOperation operation1(stagingArea, source, KeyType::kSet);
  if(!operation1.valid()) return wrong_type();

  WriteOperation operation2(stagingArea, destination, KeyType::kSet);
  if(!operation2.valid()) {
    operation1.finalize(operation1.keySize());
    return wrong_type();
  }

  if(operation1.deleteField(element)) {
    outcome = 1;
    operation1.finalize(operation1.keySize() - 1);

    if(operation2.fieldExists(element)) {
      // No-op
      operation2.finalize(operation2.keySize());
    }
    else {
      operation2.writeField(element, "1");
      operation2.finalize(operation2.keySize() + 1);
    }

    return rocksdb::Status::OK();
  }

  // No operation performed, item does not exist
  outcome = 0;
  operation1.finalize(operation1.keySize());
  operation2.finalize(operation2.keySize());

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::smembers(StagingArea &stagingArea, std::string_view key, std::vector<std::string> &members) {
  if(!assertKeyType(stagingArea, key, KeyType::kSet)) return wrong_type();

  FieldLocator locator(KeyType::kSet, key);
  members.clear();

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startsWith(tmp, locator.toView())) break;
    members.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::scard(StagingArea &stagingArea, std::string_view key, size_t &count) {
  count = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(isWrongType(keyinfo, KeyType::kSet)) return wrong_type();

  count = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::configGet(StagingArea &stagingArea, std::string_view key,  std::string &value) {
  ConfigurationLocator locator(key);
  return stagingArea.get(locator.toView(), value);
}

rocksdb::Status StateMachine::configSet(StagingArea &stagingArea, std::string_view key, std::string_view value) {
  // We don't use WriteOperation or key descriptors here,
  // since kConfiguration is special.

  std::string oldvalue = "N/A";
  rocksdb::Status st = configGet(key, oldvalue);
  if(st.ok()) oldvalue = SSTR("'" << oldvalue << "'");
  qdb_info("Applying configuration update: Key " << key << " changes from " << oldvalue << " into '" << value << "'");

  ConfigurationLocator locator(key);
  stagingArea.put(locator.toView(), value);
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::configGetall(StagingArea &stagingArea, std::vector<std::string> &res) {
  IteratorPtr iter(stagingArea.getIterator());
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

StateMachine::WriteOperation::WriteOperation(StagingArea &staging, std::string_view key, const KeyType &type)
: stagingArea(staging), redisKey(key), expectedType(type) {

  std::string tmp;

  dlocator.reset(redisKey);
  rocksdb::Status st = stagingArea.getForUpdate(dlocator.toView(), tmp);

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

    if(expectedType == KeyType::kVersionedHash) {
      keyinfo.setStartIndex(0u);
    }
  }

  finalized = !isValid;
}

bool StateMachine::WriteOperation::descriptorModifiedAlreadyInWriteBatch() {
  std::string ignored;
  rocksdb::Status st = stagingArea.readFromWriteBatch(dlocator.toView(), ignored);
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

bool StateMachine::WriteOperation::valid() {
  return isValid;
}

bool StateMachine::WriteOperation::keyExists() {
  return redisKeyExists;
}

bool StateMachine::WriteOperation::getField(std::string_view field, std::string &out) {
  assertWritable();

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  rocksdb::Status st = stagingArea.get(locator.toView(), out);
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

bool StateMachine::WriteOperation::getLocalityIndex(std::string_view field, std::string &out) {
  assertWritable();
  qdb_assert(keyinfo.getKeyType() == KeyType::kLocalityHash);

  LocalityIndexLocator locator(redisKey, field);
  rocksdb::Status st = stagingArea.get(locator.toView(), out);
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

bool StateMachine::WriteOperation::getAndDeleteLocalityIndex(std::string_view field, std::string &out) {
  assertWritable();
  qdb_assert(keyinfo.getKeyType() == KeyType::kLocalityHash);

  LocalityIndexLocator locator(redisKey, field);
  rocksdb::Status st = stagingArea.get(locator.toView(), out);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) {
    stagingArea.del(locator.toView());
  }

  return st.ok();
}

int64_t StateMachine::WriteOperation::keySize() {
  return keyinfo.getSize();
}

void StateMachine::WriteOperation::assertWritable() {
  if(!isValid) qdb_throw("WriteOperation not valid!");
  if(finalized) qdb_throw("WriteOperation already finalized!");
}

void StateMachine::WriteOperation::write(std::string_view value) {
  assertWritable();

  if(keyinfo.getKeyType() == KeyType::kString) {
    StringLocator locator(redisKey);
    stagingArea.put(locator.toView(), value);
  }
  else if(keyinfo.getKeyType() == KeyType::kLease) {
    LeaseLocator locator(redisKey);
    stagingArea.put(locator.toView(), value);
  }
  else {
    qdb_throw("writing without a field makes sense only for strings and leases");
  }
}

void StateMachine::WriteOperation::writeField(std::string_view field, std::string_view value) {
  assertWritable();

  if(keyinfo.getKeyType() != KeyType::kHash && keyinfo.getKeyType() != KeyType::kSet && keyinfo.getKeyType() != KeyType::kDeque && keyinfo.getKeyType() != KeyType::kVersionedHash) {
    qdb_throw("writing with a field makes sense only for hashes, sets, or lists");
  }

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  stagingArea.put(locator.toView(), value);
}

void StateMachine::WriteOperation::writeLocalityField(std::string_view hint, std::string_view field, std::string_view value) {
  assertWritable();

  qdb_assert(keyinfo.getKeyType() == KeyType::kLocalityHash);

  LocalityFieldLocator locator(redisKey, hint, field);
  stagingArea.put(locator.toView(), value);
}

void StateMachine::WriteOperation::writeLocalityIndex(std::string_view field, std::string_view hint) {
  assertWritable();

  qdb_assert(keyinfo.getKeyType() == KeyType::kLocalityHash);

  LocalityIndexLocator locator(redisKey, field);
  stagingArea.put(locator.toView(), hint);
}

void StateMachine::WriteOperation::cancel() {
  finalized = true;
}

rocksdb::Status StateMachine::WriteOperation::finalize(int64_t newsize, bool forceUpdate) {
  assertWritable();

  if(newsize < 0) qdb_throw("invalid newsize: " << newsize);

  if(newsize == 0 && keyinfo.getKeyType() != KeyType::kVersionedHash) {
    stagingArea.del(dlocator.toView());
  }
  else if(keyinfo.getSize() != newsize || forceUpdate) {
    keyinfo.setSize(newsize);
    stagingArea.put(dlocator.toView(), keyinfo.serialize());
  }

  finalized = true;
  return rocksdb::Status::OK(); // OK if return value is ignored
}

bool StateMachine::WriteOperation::fieldExists(std::string_view field) {
  assertWritable();

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  rocksdb::Status st = stagingArea.exists(locator.toView());
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

bool StateMachine::WriteOperation::localityFieldExists(std::string_view hint, std::string_view field) {
  assertWritable();
  qdb_assert(keyinfo.getKeyType() == KeyType::kLocalityHash);

  LocalityFieldLocator locator(redisKey, hint, field);
  rocksdb::Status st = stagingArea.exists(locator.toView());
  ASSERT_OK_OR_NOTFOUND(st);
  return st.ok();
}

bool StateMachine::WriteOperation::deleteField(std::string_view field) {
  assertWritable();

  std::string tmp;

  FieldLocator locator(keyinfo.getKeyType(), redisKey, field);
  rocksdb::Status st = stagingArea.get(locator.toView(), tmp);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) stagingArea.del(locator.toView());
  return st.ok();
}

bool StateMachine::WriteOperation::deleteLocalityField(std::string_view hint, std::string_view field) {
  assertWritable();
  qdb_assert(keyinfo.getKeyType() == KeyType::kLocalityHash);

  std::string tmp;
  LocalityFieldLocator locator(redisKey, hint, field);
  rocksdb::Status st = stagingArea.get(locator.toView(), tmp);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) stagingArea.del(locator.toView());
  return st.ok();
}

rocksdb::Status StateMachine::set(StagingArea &stagingArea, std::string_view key, std::string_view value) {
  WriteOperation operation(stagingArea, key, KeyType::kString);
  if(!operation.valid()) return wrong_type();

  operation.write(value);
  return operation.finalize(value.size());
}

rocksdb::Status StateMachine::dequePush(StagingArea &stagingArea, Direction direction, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &length) {
  WriteOperation operation(stagingArea, key, KeyType::kDeque);
  if(!operation.valid()) return wrong_type();

  KeyDescriptor &descriptor = operation.descriptor();

  uint64_t listIndex = descriptor.getListIndex(direction);
  uint64_t itemsAdded = 0;
  for(ReqIterator it = start; it != end; it++) {
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

rocksdb::Status StateMachine::dequePushFront(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &length) {
  return this->dequePush(stagingArea, Direction::kLeft, key, start, end, length);
}

rocksdb::Status StateMachine::dequePushBack(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &length) {
  return this->dequePush(stagingArea, Direction::kRight, key, start, end, length);
}

rocksdb::Status StateMachine::dequePopFront(StagingArea &stagingArea, std::string_view key, std::string &item) {
  return this->dequePop(stagingArea, Direction::kLeft, key, item);
}

rocksdb::Status StateMachine::dequePopBack(StagingArea &stagingArea, std::string_view key, std::string &item) {
  return this->dequePop(stagingArea, Direction::kRight, key, item);
}

rocksdb::Status StateMachine::dequeTrimFront(StagingArea &stagingArea, std::string_view key, std::string_view maxToKeepStr, int64_t &itemsRemoved) {
  int64_t maxToKeep;
  if(!ParseUtils::parseInt64(maxToKeepStr, maxToKeep) || maxToKeep < 0) {
    return malformed("value is not an integer or out of range");
  }

  WriteOperation operation(stagingArea, key, KeyType::kDeque);
  if(!operation.valid()) return wrong_type();

  KeyDescriptor &descriptor = operation.descriptor();

  int64_t toRemove = descriptor.getSize() - maxToKeep;
  if(toRemove <= 0) {
    operation.cancel();
    itemsRemoved = 0;
    return rocksdb::Status::OK();
  }

  int64_t eliminated = 0;
  for(uint64_t nextToEliminate = descriptor.getStartIndex()+1; nextToEliminate <=
    descriptor.getStartIndex() + toRemove; nextToEliminate++) {
    eliminated++;
    qdb_assert(operation.deleteField(unsignedIntToBinaryString(nextToEliminate)));
  }

  qdb_assert(eliminated == toRemove);
  itemsRemoved = toRemove;
  descriptor.setStartIndex(descriptor.getStartIndex() + toRemove);

  qdb_assert(descriptor.getEndIndex() - descriptor.getStartIndex() - 1 == (uint64_t) maxToKeep);
  return operation.finalize(descriptor.getEndIndex() - descriptor.getStartIndex() - 1);
}

void StateMachine::advanceClock(StagingArea &stagingArea, ClockValue newValue) {
  // Assert we're not setting the clock back..
  ClockValue prevValue;
  getClock(stagingArea, prevValue);

  if(newValue < prevValue) {
    qdb_throw("Attempted to set state machine clock in the past: " << prevValue << " ==> " << newValue);
  }

  // Clear out any leases past the deadline
  ExpirationEventIterator iter(stagingArea);

  size_t expirationEventCount = 0u;
  while(iter.valid() && iter.getDeadline() <= newValue) {
    qdb_assert(lease_release(stagingArea, std::string(iter.getRedisKey()), ClockValue(0)).ok());
    iter.next();
    expirationEventCount++;
  }

  qdb_info("Scanned through " << expirationEventCount << " expiration events when advancing clock");

  // Update value
  stagingArea.put(KeyConstants::kStateMachine_Clock, unsignedIntToBinaryString(newValue));
}

rocksdb::Status StateMachine::lease_get(StagingArea &stagingArea, std::string_view key, ClockValue clockUpdate, LeaseInfo &info) {

  // Advance clock, and clear out any expired leases.
  maybeAdvanceClock(stagingArea, clockUpdate);

  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);

  if(keyinfo.empty()) {
    return rocksdb::Status::NotFound();
  }

  if(keyinfo.getKeyType() != KeyType::kLease) {
    return wrong_type();
  }

  LeaseLocator locator(key);

  std::string value;
  THROW_ON_ERROR(stagingArea.get(locator.toView(), value));

  info = LeaseInfo(value, keyinfo.getStartIndex(), keyinfo.getEndIndex());
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::vhset(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view value, uint64_t &version) {
  WriteOperation operation(stagingArea, key, KeyType::kVersionedHash);
  if(!operation.valid()) return wrong_type();

  bool fieldcreated = !operation.fieldExists(field);
  int64_t newsize = operation.keySize() + fieldcreated;
  operation.writeField(field, value);

  // Have we modified this key in the same write batch already?
  // If yes:
  // - We have already incremented the version, nothing to do. Each transaction
  //   towards the state machine counts as a single version.
  // If not:
  // - We need to increment the version by one.
  KeyDescriptor &descriptor = operation.descriptor();
  version = descriptor.getStartIndex();

  if(!operation.descriptorModifiedAlreadyInWriteBatch()) {
    version++;
    descriptor.setStartIndex(version);
  }

  return operation.finalize(newsize, true);
}

rocksdb::Status StateMachine::vhdel(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end, uint64_t &version) {
  int64_t removed = 0;

  WriteOperation operation(stagingArea, key, KeyType::kVersionedHash);
  if(!operation.valid()) return wrong_type();

  for(ReqIterator it = start; it != end; it++) {
    removed += operation.deleteField(*it);
  }

  // Have we modified this key in the same write batch already?
  // If yes:
  // - We have already incremented the version, nothing to do. Each transaction
  //   towards the state machine counts as a single version.
  // If not:
  // - We need to increment the version by one.
  KeyDescriptor &descriptor = operation.descriptor();
  version = descriptor.getStartIndex();

  if(removed != 0 && !operation.descriptorModifiedAlreadyInWriteBatch()) {
    version++;
    descriptor.setStartIndex(version);
  }

  int64_t newsize = operation.keySize() - removed;
  return operation.finalize(newsize, true);
}

rocksdb::Status StateMachine::vhlen(StagingArea &stagingArea, std::string_view key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(isWrongType(keyinfo, KeyType::kVersionedHash)) return wrong_type();

  len = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::vhgetall(StagingArea &stagingArea, std::string_view key, std::vector<std::string> &res, uint64_t &version) {
  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);

  if(keyinfo.empty()) {
    res.clear();
    version = 0u;
    return rocksdb::Status::OK();
  }

  if(keyinfo.getKeyType() != KeyType::kVersionedHash) {
    return wrong_type();
  }

  res.clear();
  FieldLocator locator(KeyType::kVersionedHash, key);

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startsWith(tmp, locator.toView())) break;
    res.push_back(std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()));
    res.push_back(iter->value().ToString());
  }

  version = keyinfo.getStartIndex();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::hclone(StagingArea &stagingArea, std::string_view source, std::string_view target) {
  WriteOperation operation(stagingArea, target, KeyType::kHash);
  if(!operation.valid()) return wrong_type();
  if(operation.keyExists()) {
    operation.cancel();
    return rocksdb::Status::InvalidArgument("ERR target key already exists, will not overwrite");
  }

  KeyDescriptor sourceKeyInfo = getKeyDescriptor(stagingArea, source);
  if(sourceKeyInfo.empty()) {
    operation.cancel();
    return rocksdb::Status::OK(); // source key is empty, do nothing
  }

  if(sourceKeyInfo.getKeyType() != KeyType::kHash) {
    operation.cancel();
    return wrong_type();
  }

  int64_t newsize = 0;
  FieldLocator locator(KeyType::kHash, source);

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.getPrefix()); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!StringUtils::startsWith(tmp, locator.toView())) break;

    operation.writeField(
      std::string(tmp.begin()+locator.getPrefixSize(), tmp.end()),
      iter->value().ToString()
    );
    newsize++;
  }

  qdb_assert(newsize == sourceKeyInfo.getSize());
  return operation.finalize(newsize);
}

void StateMachine::advanceClock(ClockValue newValue, LogIndex index) {
  StagingArea stagingArea(*this);
  advanceClock(stagingArea, newValue);
  stagingArea.commit(index);
}

ClockValue StateMachine::maybeAdvanceClock(StagingArea &stagingArea, ClockValue clockUpdate) {
  // Get current clock time.
  ClockValue currentClock;
  getClock(stagingArea, currentClock);

  // Two cases:
  // - currentClock is behind clockUpdate - should be by far the most common.
  //   Simply update currentClock to clockUpdate.
  // - currentClock is ahead.. we were hit by a rare race condition. Advance
  //   clockUpdate to currentClock instead.
  if(currentClock < clockUpdate) {
    advanceClock(stagingArea, clockUpdate);
    return clockUpdate;
  }
  else {
    return currentClock;
  }
}

void StateMachine::getClock(StagingArea &stagingArea, ClockValue &value) {
  std::string prevValue;
  THROW_ON_ERROR(stagingArea.get(KeyConstants::kStateMachine_Clock, prevValue));

  if(prevValue.size() != 8u) {
    qdb_throw("Clock corruption, expected exactly 8 bytes, got " << prevValue.size());
  }

  value = binaryStringToUnsignedInt(prevValue.c_str());
}

void StateMachine::lease_get_pending_expiration_events(StagingArea &stagingArea, ClockValue &staticClock, ClockValue &dynamicClock, std::vector<ExpirationEvent> &events) {
  events.clear();

  getClock(stagingArea, staticClock);
  dynamicClock = getDynamicClock();

  ExpirationEventIterator iter(stagingArea);

  while(iter.valid()) {
    events.emplace_back(iter.getRedisKey(), iter.getDeadline());
    iter.next();
  }
}

void StateMachine::getType(StagingArea &stagingArea, std::string_view key, std::string &keyType) {
  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  keyType = keyTypeAsString(keyinfo.getKeyType());
}

void StateMachine::getClock(ClockValue &value) {
  StagingArea stagingArea(*this, true);
  getClock(stagingArea, value);
}

LeaseAcquisitionStatus StateMachine::lease_acquire(StagingArea &stagingArea, std::string_view key, std::string_view value, ClockValue clockUpdate, uint64_t duration, LeaseInfo &info) {
  qdb_assert(!value.empty());

  // First, some timekeeping, update clock time if necessary.
  clockUpdate = maybeAdvanceClock(stagingArea, clockUpdate);

  // Ensure the key pointed to is either a lease, or non-existent.
  WriteOperation operation(stagingArea, key, KeyType::kLease);
  if(!operation.valid()) return LeaseAcquisitionStatus::kKeyTypeMismatch;

  // Quick check that no-one else holds the lease right now.
  // Could it be that the lease has actually expired? Not at this point.
  // advanceClock() should have taken care of removing expired leases.

  LeaseLocator locator(key);
  std::string oldLeaseHolder;
  rocksdb::Status st = stagingArea.get(locator.toView(), oldLeaseHolder);
  ASSERT_OK_OR_NOTFOUND(st);

  if(st.ok()) {
    if(oldLeaseHolder != value) {
      KeyDescriptor &descriptor = operation.descriptor();
      info = LeaseInfo(oldLeaseHolder, descriptor.getStartIndex(), descriptor.getEndIndex());
      operation.cancel();
      return LeaseAcquisitionStatus::kFailedDueToOtherOwner;
    }
  }

  // Looks good.. Either the lease is held by the same holder, and this is
  // simply an extension request, or this is a new lease altogether.

  KeyDescriptor &descriptor = operation.descriptor();
  bool extended = operation.keyExists();
  if(operation.keyExists()) {
    // Lease extension.. need to wipe out old pending expiration event
    ExpirationEventLocator oldEvent(descriptor.getEndIndex(), key);
    THROW_ON_ERROR(stagingArea.exists(oldEvent.toView()));
    stagingArea.del(oldEvent.toView());
  }

  // Anchor expiration timestamp based on clockUpdate.
  ClockValue expirationTimestamp = clockUpdate + duration;
  descriptor.setStartIndex(clockUpdate);
  descriptor.setEndIndex(expirationTimestamp);

  // Store expiration event.
  ExpirationEventLocator newEvent(expirationTimestamp, key);
  stagingArea.put(newEvent.toView(), "1");

  // Update lease value.
  operation.write(value);
  info = LeaseInfo(value, descriptor.getStartIndex(), descriptor.getEndIndex());

  operation.finalize(value.size(), true);
  if(extended) return LeaseAcquisitionStatus::kRenewed;
  return LeaseAcquisitionStatus::kAcquired;
}

rocksdb::Status StateMachine::lease_release(StagingArea &stagingArea, std::string_view key, ClockValue clockUpdate) {
  // First, some timekeeping, update clock time if necessary.
  if(clockUpdate != 0u) {
    // maybeAdvanceClock will also call this function.. Avoid infinite loop
    // by supplying clockUpdate == 0u.
    maybeAdvanceClock(stagingArea, clockUpdate);
  }

  WriteOperation operation(stagingArea, key, KeyType::kLease);
  if(!operation.valid()) return wrong_type();

  if(!operation.keyExists()) {
    operation.finalize(0u);
    return rocksdb::Status::NotFound();
  }

  KeyDescriptor &descriptor = operation.descriptor();

  ExpirationEventLocator event(descriptor.getEndIndex(), key);
  THROW_ON_ERROR(stagingArea.exists(event.toView()));
  stagingArea.del(event.toView());

  LeaseLocator leaseLocator(key);
  THROW_ON_ERROR(stagingArea.exists(leaseLocator.toView()));
  stagingArea.del(leaseLocator.toView());

  return operation.finalize(0u);
}

rocksdb::Status StateMachine::dequeLen(StagingArea &stagingArea, std::string_view key, size_t &len) {
  len = 0;

  KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, key);
  if(isWrongType(keyinfo, KeyType::kDeque)) return wrong_type();

  len = keyinfo.getSize();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::dequePop(StagingArea &stagingArea, Direction direction, std::string_view key, std::string &item) {
  WriteOperation operation(stagingArea, key, KeyType::kDeque);
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

rocksdb::Status StateMachine::get(StagingArea &stagingArea, std::string_view key, std::string &value) {
  if(!assertKeyType(stagingArea, key, KeyType::kString)) return wrong_type();

  StringLocator slocator(key);
  return stagingArea.get(slocator.toView(), value);
}

void StateMachine::remove_all_with_prefix(std::string_view prefix, int64_t &removed, StagingArea &stagingArea) {
  removed = 0;

  std::string tmp;
  IteratorPtr iter(stagingArea.getIterator());

  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    // iter->key() may get deleted from under our feet, better keep a copy
    std::string key = iter->key().ToString();
    if(!StringUtils::startsWith(key, prefix)) break;
    if(key.size() > 0 && (key[0] == char(InternalKeyType::kInternal) || key[0] == char(InternalKeyType::kConfiguration))) continue;

    stagingArea.del(key);
    removed++;
  }
}

rocksdb::Status StateMachine::del(StagingArea &stagingArea, const ReqIterator &start, const ReqIterator &end, int64_t &removed) {
  removed = 0;

  for(ReqIterator it = start; it != end; it++) {
    DescriptorLocator dlocator(*it);
    KeyDescriptor keyInfo = lockKeyDescriptor(stagingArea, dlocator);
    if(keyInfo.empty()) continue;

    std::string tmp;

    if(keyInfo.getKeyType() == KeyType::kString) {
      StringLocator slocator(*it);
      THROW_ON_ERROR(stagingArea.get(slocator.toView(), tmp));
      stagingArea.del(slocator.toView());
    }
    else if(keyInfo.getKeyType() == KeyType::kHash || keyInfo.getKeyType() == KeyType::kSet || keyInfo.getKeyType() == KeyType::kDeque || keyInfo.getKeyType() == KeyType::kVersionedHash) {
      FieldLocator locator(keyInfo.getKeyType(), *it);
      int64_t count = 0;
      remove_all_with_prefix(locator.toView(), count, stagingArea);
      if(count != keyInfo.getSize()) qdb_throw("mismatch between keyInfo counter and number of elements deleted by remove_all_with_prefix: " << count << " vs " << keyInfo.getSize());
    }
    else if(keyInfo.getKeyType() == KeyType::kLocalityHash) {
      // wipe out fields
      LocalityFieldLocator fieldLocator(*it);
      int64_t count = 0;
      remove_all_with_prefix(fieldLocator.toView(), count, stagingArea);
      if(count != keyInfo.getSize()) qdb_throw("mismatch between keyInfo counter and number of elements deleted by remove_all_with_prefix: " << count << " vs " << keyInfo.getSize());

      // wipe out indexes
      LocalityIndexLocator indexLocator(*it);
      count = 0;
      remove_all_with_prefix(indexLocator.toView(), count, stagingArea);
      if(count != keyInfo.getSize()) qdb_throw("mismatch between keyInfo counter and number of elements deleted by remove_all_with_prefix: " << count << " vs " << keyInfo.getSize());
    }
    else {
      qdb_throw("DEL called on unknown keytype - should never happen");
    }

    removed++;
    stagingArea.del(dlocator.toView());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::exists(StagingArea &stagingArea, const ReqIterator &start, const ReqIterator &end, int64_t &count) {
  count = 0;

  for(auto it = start; it != end; it++) {
    KeyDescriptor keyinfo = getKeyDescriptor(stagingArea, *it);

    if(!keyinfo.empty()) {
      count++;
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::keys(StagingArea &stagingArea, std::string_view pattern, std::vector<std::string> &result) {
  result.clear();

  bool allkeys = (pattern.length() == 1 && pattern[0] == '*');
  IteratorPtr iter(stagingArea.getIterator());

  std::string searchPrefix(1, char(InternalKeyType::kDescriptor));
  for(iter->Seek(searchPrefix); iter->Valid(); iter->Next()) {
    std::string rkey = iter->key().ToString();
    if(rkey.size() == 0 || rkey[0] != char(InternalKeyType::kDescriptor)) break;

    if(allkeys || stringmatchlen(pattern.data(), pattern.length(), rkey.data()+1, rkey.length()-1, 0)) {
      result.push_back(rkey.substr(1));
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::scan(StagingArea &stagingArea, std::string_view cursor, std::string_view pattern, size_t count, std::string &newcursor, std::vector<std::string> &results) {
  results.clear();

  // Any hits *must* start with patternPrefix. This will allow us in many
  // circumstances to eliminate checking large parts of the keyspace, without
  // having to call stringmatchlen.
  // Best-case pattern is "sometext*", where there are no wasted iterations.
  std::string patternPrefix = extractPatternPrefix(pattern);

  DescriptorLocator locator;
  if(cursor.empty()) {
    locator.reset(patternPrefix);
  }
  else {
    locator.reset(cursor);
  }

  size_t iterations = 0;
  bool emptyPattern = (pattern.empty() || pattern == "*");

  IteratorPtr iter(stagingArea.getIterator());
  for(iter->Seek(locator.toView()); iter->Valid(); iter->Next()) {
    iterations++;

    std::string rkey = iter->key().ToString();

    // Check if we should terminate the search
    if(rkey.size() == 0 || rkey[0] != char(InternalKeyType::kDescriptor)) break;
    if(!StringUtils::isPrefix(patternPrefix, rkey.c_str()+1, rkey.size()-1)) {
      // Take a shortcut and break scanning early,
      // since no more matches can possibly exist.
      break;
    }

    if(iterations > count) {
      newcursor = rkey.substr(1);
      return rocksdb::Status::OK();
    }

    if(emptyPattern || stringmatchlen(pattern.data(), pattern.length(), rkey.data()+1, rkey.length()-1, 0)) {
      results.push_back(rkey.substr(1));
    }
  }

  newcursor.clear();
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::flushall(StagingArea &stagingArea) {
  int64_t tmp;
  remove_all_with_prefix("", tmp, stagingArea);
  return rocksdb::Status::OK();
}

rocksdb::Status StateMachine::checkpoint(const std::string &path) {
  rocksdb::Checkpoint *checkpoint = nullptr;
  RETURN_ON_ERROR(rocksdb::Checkpoint::Create(db.get(), &checkpoint));

  rocksdb::Status st = checkpoint->CreateCheckpoint(path);
  delete checkpoint;

  return st;
}

std::string StateMachine::statistics() {
  std::string stats;
  db->GetProperty("rocksdb.stats", &stats);
  return stats;
}

std::string StateMachine::levelStats() {
  std::string stats;
  db->GetProperty(rocksdb::DB::Properties::kLevelStats, &stats);
  return stats;
}

std::vector<std::string> StateMachine::compressionStats() {
  std::vector<std::string> results;

  for(size_t i = 0; i <= 6; i++) {
    std::string tmp;
    db->GetProperty(SSTR(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix << i), &tmp);
    results.emplace_back(tmp);
  }

  return results;
}

rocksdb::Status StateMachine::noop(LogIndex index) {
  StagingArea stagingArea(*this);
  return stagingArea.commit(index);
}

rocksdb::Status StateMachine::manualCompaction() {
  qdb_event("Triggering manual compaction.. auto-compaction will be disabled while the manual one is running.");
  // Disabling auto-compactions is a hack to prevent write-stalling. Pending compaction
  // bytes will jump to the total size of the DB as soon as a manual compaction is
  // issued, which will most likely stall or completely stop writes for a long time.
  // (depends on the size of the DB)
  // This is a recommendation by rocksdb devs as a workaround: Disabling auto
  // compactions will disable write-stalling as well.
  THROW_ON_ERROR(db->SetOptions( { {"disable_auto_compactions", "true"} } ));

  rocksdb::CompactRangeOptions opts;
  opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;

  rocksdb::Status st = db->CompactRange(opts, nullptr, nullptr);

  THROW_ON_ERROR(db->SetOptions( { {"disable_auto_compactions", "false"} } ));
  return st;
}

void StateMachine::finalizeBulkload() {
  qdb_event("Finalizing bulkload, issuing manual compaction...");
  THROW_ON_ERROR(manualCompaction());
  qdb_event("Manual compaction was successful. Building key descriptors...");
  KeyDescriptorBuilder builder(*this);
  THROW_ON_ERROR(db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_InBulkload, boolToString(false)));
  qdb_event("All done, bulkload is over. Restart quarkdb in standalone mode.");
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

rocksdb::Status StateMachine::verifyChecksum() {
  qdb_info("Initiating a full checksum scan of the state machine.");

  std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
  rocksdb::Status status = db->VerifyChecksum();
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  std::chrono::seconds duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  if(status.ok()) {
    qdb_info("State machine checksum scan successful! (took " << formatTime(duration) << ")");
  }
  else {
    qdb_critical("State machine corruption, checksum verification failed: " << status.ToString());
  }

  return status;
}

bool StateMachine::waitUntilTargetLastApplied(LogIndex targetLastApplied, std::chrono::milliseconds duration) {
  std::unique_lock<std::mutex> lock(lastAppliedMtx);

  if(targetLastApplied <= lastApplied) {
    return true;
  }

  lastAppliedCV.wait_for(lock, duration);
  return (targetLastApplied <= lastApplied);
}

//------------------------------------------------------------------------------
// Extremely dangerous operation, the state-machine should NOT be part
// of an active raft-machinery when this function is called, or even facing
// client traffic at all.
//------------------------------------------------------------------------------
void StateMachine::forceResetLastApplied(LogIndex newLastApplied) {
  std::lock_guard<std::mutex> lock(lastAppliedMtx);
  qdb_info("Resetting lastApplied for state-machine stored in '" << filename << "': " << lastApplied << " => " << newLastApplied);
  THROW_ON_ERROR(db->Put(rocksdb::WriteOptions(), KeyConstants::kStateMachine_LastApplied, intToBinaryString(newLastApplied)));
  lastApplied = newLastApplied;
}

void StateMachine::commitTransaction(rocksdb::WriteBatchWithIndex &wb, LogIndex index) {
  std::lock_guard<std::mutex> lock(lastAppliedMtx);

  if(index <= 0 && lastApplied > 0) qdb_throw("provided invalid index for version-tracked database: " << index << ", current last applied: " << lastApplied);

  if(index > 0) {
    if(index != lastApplied+1) qdb_throw("attempted to perform illegal lastApplied update: " << lastApplied << " ==> " << index);
    THROW_ON_ERROR(wb.Put(KeyConstants::kStateMachine_LastApplied, intToBinaryString(index)));
  }

  rocksdb::WriteOptions opts;
  opts.disableWAL = !writeAheadLog;

  rocksdb::Status st = db->Write(opts, wb.GetWriteBatch() );
  if(index > 0 && st.ok()) lastApplied = index;
  if(!st.ok()) qdb_throw("unable to commit transaction with index " << index << ": " << st.ToString());

  // Notify that last applied has changed
  lastAppliedCV.notify_all();
}

// Simple API for writes - chain to pipelined API using a single operation per batch

#define CHAIN(index, func, ...) { \
  StagingArea stagingArea(*this); \
  auto st = this->func(stagingArea, ## __VA_ARGS__); \
  stagingArea.commit(index); \
  return st; \
}

#define CHAIN_READ(func, ...) { \
  StagingArea stagingArea(*this, true); \
  return this->func(stagingArea, ## __VA_ARGS__); \
}

//------------------------------------------------------------------------------
// Convenience functions, without having to manually instantiate a staging area.
// Reads:
//------------------------------------------------------------------------------

rocksdb::Status StateMachine::get(std::string_view key, std::string &value) {
  CHAIN_READ(get, key, value);
}

rocksdb::Status StateMachine::exists(const ReqIterator &start, const ReqIterator &end, int64_t &count) {
  CHAIN_READ(exists, start, end, count);
}

rocksdb::Status StateMachine::keys(std::string_view pattern, std::vector<std::string> &result) {
  CHAIN_READ(keys, pattern, result);
}

rocksdb::Status StateMachine::scan(std::string_view cursor, std::string_view pattern, size_t count, std::string &newcursor, std::vector<std::string> &results) {
  CHAIN_READ(scan, cursor, pattern, count, newcursor, results);
}

rocksdb::Status StateMachine::hget(std::string_view key, std::string_view field, std::string &value) {
  CHAIN_READ(hget, key, field, value);
}

rocksdb::Status StateMachine::hexists(std::string_view key, std::string_view field) {
  CHAIN_READ(hexists, key, field);
}

rocksdb::Status StateMachine::hkeys(std::string_view key, std::vector<std::string> &keys) {
  CHAIN_READ(hkeys, key, keys);
}

rocksdb::Status StateMachine::hgetall(std::string_view key, std::vector<std::string> &res) {
  CHAIN_READ(hgetall, key, res);
}

rocksdb::Status StateMachine::hlen(std::string_view key, size_t &len) {
  CHAIN_READ(hlen, key, len);
}

rocksdb::Status StateMachine::hvals(std::string_view key, std::vector<std::string> &vals) {
  CHAIN_READ(hvals, key, vals);
}

rocksdb::Status StateMachine::hscan(std::string_view key, std::string_view cursor, size_t count, std::string &newcursor, std::vector<std::string> &results) {
  CHAIN_READ(hscan, key, cursor, count, newcursor, results);
}

rocksdb::Status StateMachine::sismember(std::string_view key, std::string_view element) {
  CHAIN_READ(sismember, key, element);
}

rocksdb::Status StateMachine::smembers(std::string_view key, std::vector<std::string> &members) {
  CHAIN_READ(smembers, key, members);
}

rocksdb::Status StateMachine::scard(std::string_view key, size_t &count) {
  CHAIN_READ(scard, key, count);
}

rocksdb::Status StateMachine::sscan(std::string_view key, std::string_view cursor, size_t count, std::string &newCursor, std::vector<std::string> &res) {
  CHAIN_READ(sscan, key, cursor, count, newCursor, res);
}

rocksdb::Status StateMachine::dequeLen(std::string_view key, size_t &len) {
  CHAIN_READ(dequeLen, key, len);
}

rocksdb::Status StateMachine::configGet(std::string_view key, std::string &value) {
  CHAIN_READ(configGet, key, value);
}

rocksdb::Status StateMachine::configGetall(std::vector<std::string> &res) {
  CHAIN_READ(configGetall, res);
}

rocksdb::Status StateMachine::lhlen(std::string_view key, size_t &len) {
  CHAIN_READ(lhlen, key, len);
}

rocksdb::Status StateMachine::lhget(std::string_view key, std::string_view field, std::string_view hint, std::string &value) {
  CHAIN_READ(lhget, key, field, hint, value);
}

rocksdb::Status StateMachine::vhgetall(std::string_view key, std::vector<std::string> &res, uint64_t &version) {
  CHAIN_READ(vhgetall, key, res, version);
}

//------------------------------------------------------------------------------
// Writes:
//------------------------------------------------------------------------------

rocksdb::Status StateMachine::hset(std::string_view key, std::string_view field, std::string_view value, bool &fieldcreated, LogIndex index) {
  CHAIN(index, hset, key, field, value, fieldcreated);
}

rocksdb::Status StateMachine::hmset(std::string_view key, const ReqIterator &start, const ReqIterator &end, LogIndex index) {
  CHAIN(index, hmset, key, start, end);
}

rocksdb::Status StateMachine::hsetnx(std::string_view key, std::string_view field, std::string_view value, bool &fieldcreated, LogIndex index) {
  CHAIN(index, hsetnx, key, field, value, fieldcreated);
}

rocksdb::Status StateMachine::hincrby(std::string_view key, std::string_view field, std::string_view incrby, int64_t &result, LogIndex index) {
  CHAIN(index, hincrby, key, field, incrby, result);
}

rocksdb::Status StateMachine::hincrbyfloat(std::string_view key, std::string_view field, std::string_view incrby, double &result, LogIndex index) {
  CHAIN(index, hincrbyfloat, key, field, incrby, result);
}

rocksdb::Status StateMachine::hdel(std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &removed, LogIndex index) {
  CHAIN(index, hdel, key, start, end, removed);
}

rocksdb::Status StateMachine::sadd(std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &added, LogIndex index) {
  CHAIN(index, sadd, key, start, end, added);
}

rocksdb::Status StateMachine::srem(std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &removed, LogIndex index) {
  CHAIN(index, srem, key, start, end, removed);
}

rocksdb::Status StateMachine::set(std::string_view key, std::string_view value, LogIndex index) {
  CHAIN(index, set, key, value);
}

rocksdb::Status StateMachine::del(const ReqIterator &start, const ReqIterator &end, int64_t &removed, LogIndex index) {
  CHAIN(index, del, start, end, removed);
}

rocksdb::Status StateMachine::flushall(LogIndex index) {
  CHAIN(index, flushall);
}

rocksdb::Status StateMachine::dequePopFront(std::string_view key, std::string &item, LogIndex index) {
  CHAIN(index, dequePopFront, key, item);
}

rocksdb::Status StateMachine::dequePopBack(std::string_view key, std::string &item, LogIndex index) {
  CHAIN(index, dequePopBack, key, item);
}

rocksdb::Status StateMachine::dequePushFront(std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &length, LogIndex index) {
  CHAIN(index, dequePushFront, key, start, end, length);
}

rocksdb::Status StateMachine::dequePushBack(std::string_view key, const ReqIterator &start, const ReqIterator &end, int64_t &length, LogIndex index) {
  CHAIN(index, dequePushBack, key, start, end, length);
}

rocksdb::Status StateMachine::configSet(std::string_view key, std::string_view value, LogIndex index) {
  CHAIN(index, configSet, key, value);
}

rocksdb::Status StateMachine::lhset(std::string_view key, std::string_view field, std::string_view hint, std::string_view value, bool &fieldcreated, LogIndex index) {
  CHAIN(index, lhset, key, field, hint, value, fieldcreated);
}

LeaseAcquisitionStatus StateMachine::lease_acquire(std::string_view key, std::string_view value, ClockValue clockUpdate, uint64_t duration, LeaseInfo &info, LogIndex index) {
  CHAIN(index, lease_acquire, key, value, clockUpdate, duration, info);
}

rocksdb::Status StateMachine::lease_get(std::string_view key, ClockValue clockUpdate, LeaseInfo &info, LogIndex index) {
  CHAIN(index, lease_get, key, clockUpdate, info);
}

rocksdb::Status StateMachine::lease_release(std::string_view key, ClockValue clockUpdate,  LogIndex index) {
  CHAIN(index, lease_release, key, clockUpdate);
}

rocksdb::Status StateMachine::dequeTrimFront(std::string_view key, std::string_view maxToKeep, int64_t &itemsRemoved, LogIndex index) {
  CHAIN(index, dequeTrimFront, key, maxToKeep, itemsRemoved);
}

rocksdb::Status StateMachine::vhset(std::string_view key, std::string_view field, std::string_view value, uint64_t &version, LogIndex index) {
  CHAIN(index, vhset, key, field, value, version);
}
