// ----------------------------------------------------------------------
// File: ReverseLocator.hh
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

#ifndef QUARKDB_REVERSE_LOCATOR_H
#define QUARKDB_REVERSE_LOCATOR_H

#include "KeyDescriptor.hh"
#include "KeyLocators.hh"

namespace quarkdb {

inline size_t extractKey(const rocksdb::Slice &dkey, std::string &key) {
  key.clear();
  key.reserve(dkey.size());

  for(size_t i = 1; i < dkey.size(); i++) {
    // Hash?
    if(dkey[i] == '#') {
      // Is this the boundary?
      if(dkey[i+1] == '#' && dkey[i-1] != '|') {
        // Yes, our work here is done.
        return i+2;
      }

      qdb_assert(dkey[i-1] == '|');
      // Replace previous character with '#'
      key[key.size() - 1] = '#';
    }
    else {
      // Just append the character to the end of the string
      key.append(1, dkey.data()[i]);
    }
  }

  qdb_critical("Parse error, unable to extract original redis key from '" << dkey.ToString() << "'");
  return 0;
}

// Given an encoded rocksdb key, extract original key (and field, if available)
// The underlying memory of given slice must remain alive while this object is
// being accessed.
class ReverseLocator {
public:
  ReverseLocator() {}

  ReverseLocator(const rocksdb::Slice sl) : slice(sl) {
    keyType = parseKeyType(sl.data()[0]);
    if(keyType == KeyType::kParseError || keyType == KeyType::kString) {
      return;
    }

    // This is a key + field then.. Need to tell them apart.
    for(size_t i = 0; i < slice.size(); i++) {
      if(slice.data()[i] == '#' && slice.data()[i-1] == '|') {
        // Oriignal key contains escaped hashes, do heavyweight parsing.
        fieldStart = extractKey(slice, unescapedKey);
        if(fieldStart == 0) keyType = KeyType::kParseError;
        return;
      }

      if(slice.data()[i] == '#' && slice.data()[i+1] == '#') {
        // No escaped hashes, yay
        fieldStart = i+2;
        return;
      }
    }

    // This shouldn't happen.. flag parse error
    keyType = KeyType::kParseError;
  }

  KeyType getKeyType() {
    return keyType;
  }

  rocksdb::Slice getOriginalKey() {
    qdb_assert(keyType != KeyType::kParseError);
    if(!unescapedKey.empty()) {
      return rocksdb::Slice(unescapedKey.data(), unescapedKey.size());
    }

    if(keyType == KeyType::kString) {
      return rocksdb::Slice(slice.data()+1, slice.size()-1);
    }

    return rocksdb::Slice(slice.data()+1, fieldStart-3);
  }

  rocksdb::Slice getField() {
    qdb_assert(keyType != KeyType::kParseError && keyType != KeyType::kString);
    return rocksdb::Slice(slice.data()+fieldStart, slice.size()-fieldStart);
  }

  rocksdb::Slice getRawPrefix() {
    qdb_assert(keyType != KeyType::kParseError && keyType != KeyType::kString);
    return rocksdb::Slice(slice.data(), fieldStart);
  }

  bool isLocalityIndex() {
    if(keyType != KeyType::kLocalityHash) return false;

    rocksdb::Slice field = getField();
    qdb_assert(field.size() != 0u);
    return *(field.data()) == char(InternalLocalityFieldType::kIndex);
  }

private:
  rocksdb::Slice slice;

  KeyType keyType = KeyType::kParseError;
  std::string unescapedKey;
  size_t fieldStart;
};

}

#endif
