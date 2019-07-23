// ----------------------------------------------------------------------
// File: RecoveryEditor.cc
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

#include "../storage/KeyConstants.hh"
#include "RecoveryEditor.hh"
#include "../Utils.hh"
#include "../utils/StringUtils.hh"
#include "../storage/InternalKeyParsing.hh"
#include <rocksdb/status.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

using namespace quarkdb;

RecoveryEditor::RecoveryEditor(std::string_view path_) : path(path_) {
  qdb_event("RECOVERY EDITOR: Opening rocksdb database at " << quotes(path));
  rocksdb::DB *tmpdb;

  rocksdb::Options options;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &tmpdb);

  if(!status.ok()) qdb_throw("Cannot open " << quotes(path) << ":" << status.ToString());
  db.reset(tmpdb);
}

RecoveryEditor::~RecoveryEditor() {
  if(db) {
    qdb_event("RECOVERY EDITOR: Closing rocksdb database at " << quotes(path));
    db.reset();
  }
}

std::vector<std::string> RecoveryEditor::retrieveMagicValues() {
  std::vector<std::string> results;

  for(auto it = KeyConstants::allKeys.begin(); it != KeyConstants::allKeys.end(); it++) {
    std::string tmp;
    rocksdb::Status st = db->Get(rocksdb::ReadOptions(), *it, &tmp);

    if(st.ok()) {
      results.emplace_back(*it);
      results.emplace_back(tmp);
    }
    else {
      results.emplace_back(SSTR(*it << ": " << st.ToString()));
    }
  }

  return results;
}

rocksdb::Status RecoveryEditor::get(std::string_view key, std::string &value) {
  return db->Get(rocksdb::ReadOptions(), key, &value);
}

rocksdb::Status RecoveryEditor::set(std::string_view key, std::string_view value) {
  return db->Put(rocksdb::WriteOptions(), key, value);
}

rocksdb::Status RecoveryEditor::del(std::string_view key) {
  std::string tmp;

  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), key, &tmp);

  if(st.IsNotFound()) {
    rocksdb::Status st2 = db->Delete(rocksdb::WriteOptions(), key);
    return rocksdb::Status::InvalidArgument("key not found, but I inserted a tombstone anyway. Deletion status: " + st2.ToString());
  }

  if(!st.ok()) {
    return st;
  }

  return db->Delete(rocksdb::WriteOptions(), key);
}

using IteratorPtr = std::unique_ptr<rocksdb::Iterator>;

rocksdb::Status RecoveryEditor::scan(std::string_view key, size_t count, std::string &nextCursor, std::vector<std::string> &elements) {
  rocksdb::ReadOptions opts;
  opts.iter_start_seqnum = 1;

  IteratorPtr iter(db->NewIterator(opts));
  iter->Seek(key);

  size_t processed = 0;
  while(iter->Valid()) {
    std::string_view currentKey = iter->key().ToStringView();

    std::string internalKeyType = getInternalKeyType(currentKey);
    currentKey.remove_suffix(8u);

    if(processed >= count) {
      nextCursor = currentKey;
      break;
    }

    elements.emplace_back(SSTR("TYPE: " << internalKeyType));
    elements.emplace_back(SSTR("KEY: " << currentKey));
    elements.emplace_back(SSTR("VALUE: " << iter->value().ToStringView()));

    processed++;
    iter->Next();
  }

  return rocksdb::Status::OK();
}
