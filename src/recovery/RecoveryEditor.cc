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
#include <rocksdb/status.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>

using namespace quarkdb;

RecoveryEditor::RecoveryEditor(const std::string &path_) : path(path_) {
  qdb_event("RECOVERY EDITOR: Opening rocksdb database at " << quotes(path));

  rocksdb::Options options;
  options.create_if_missing = false;
  rocksdb::Status status = rocksdb::DB::Open(options, path, &db);

  if(!status.ok()) qdb_throw("Cannot open " << quotes(path) << ":" << status.ToString());
}

RecoveryEditor::~RecoveryEditor() {
  if(db) {
    qdb_event("RECOVERY EDITOR: Closing rocksdb database at " << quotes(path));
    delete db;
    db = nullptr;
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

rocksdb::Status RecoveryEditor::get(const std::string &key, std::string &value) {
  return db->Get(rocksdb::ReadOptions(), key, &value);
}
