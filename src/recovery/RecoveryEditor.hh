// ----------------------------------------------------------------------
// File: RecoveryEditor.hh
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

#ifndef __QUARKDB_RECOVERY_EDITOR_H__
#define __QUARKDB_RECOVERY_EDITOR_H__

#include <vector>
#include <rocksdb/db.h>

namespace quarkdb {

//------------------------------------------------------------------------------
// A class to allow raw access to rocksdb.
//------------------------------------------------------------------------------

class RecoveryEditor {
public:
  RecoveryEditor(std::string_view path);
  ~RecoveryEditor();

  std::vector<std::string> retrieveMagicValues();
  rocksdb::Status get(std::string_view key, std::string &value);
  rocksdb::Status set(std::string_view key, std::string_view value);
  rocksdb::Status del(std::string_view key);
  rocksdb::Status scan(std::string_view key, size_t count, std::string &nextCursor, std::vector<std::string> &elements);

private:
  std::string path;
  std::unique_ptr<rocksdb::DB> db;
};

}

#endif
