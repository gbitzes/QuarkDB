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

inline size_t extractPrefix(std::string_view dkey, std::string &key) {
  key.clear();
  key.reserve(dkey.size());

  for(size_t i = 0; i < dkey.size(); i++) {
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

  qdb_critical("Parse error, unable to extract original redis key from '" << dkey << "'");
  return 0;
}

// Given a slice containing an escaped prefix, extract the original, unescaped
// value and the boundary.
class EscapedPrefixExtractor {
public:
  EscapedPrefixExtractor() {}

  bool parse(std::string_view sl) {
    parsingOk = false;
    slice = sl;
    unescaped.clear();
    boundary = 0;

    for(size_t i = 1; i < slice.size(); i++) {
      if(slice.data()[i] == '#' && slice.data()[i-1] == '|') {
        // Original prefix contains escaped hashes, do heavyweight parsing.
        boundary = extractPrefix(slice, unescaped);
        if(boundary == 0) return false; // parse error

        parsingOk = true;
        return true;
      }

      if(slice.data()[i] == '#' && slice.data()[i-1] == '#') {
        // No escaped hashes, yay. Zero-copy case.
        boundary = i+1;

        parsingOk = true;
        return true;
      }
    }

    // We shouldn't normally reach here.. parse error
    return false;
  }

  std::string_view getOriginalPrefix() const {
    qdb_assert(parsingOk);

    if(!unescaped.empty()) {
      return unescaped;
    }

    return std::string_view(slice.data(), boundary-2);
  }


  std::string_view getRawPrefix() const {
    qdb_assert(parsingOk);
    return std::string_view(slice.data(), boundary-2);
  }

  std::string_view getRawSuffix() const {
    qdb_assert(parsingOk);
    return std::string_view(slice.data()+boundary, slice.size()-boundary);
  }

  size_t getBoundary() const {
    qdb_assert(parsingOk);
    return boundary;
  }

private:
  bool parsingOk;
  std::string_view slice;

  std::string unescaped;
  size_t boundary;
};

// Given an encoded rocksdb key, extract original key (and field, if available)
// The underlying memory of given slice must remain alive while this object is
// being accessed.
class ReverseLocator {
public:
  ReverseLocator() {}

  ReverseLocator(std::string_view sl) : slice(sl) {
    keyType = parseKeyType(sl.data()[0]);
    if(keyType == KeyType::kParseError || keyType == KeyType::kString) {
      return;
    }

    std::string_view withoutKeyType = slice;
    withoutKeyType.remove_prefix(1);

    // Extract first chunk.
    if(!firstChunk.parse(withoutKeyType) || firstChunk.getBoundary() == 0u) {
      keyType = KeyType::kParseError;
      return;
    }
  }

  KeyType getKeyType() {
    return keyType;
  }

  std::string_view getOriginalKey() {
    qdb_assert(keyType != KeyType::kParseError);

    if(keyType == KeyType::kString) {
      return std::string_view(slice.data()+1, slice.size()-1);
    }

    return firstChunk.getOriginalPrefix();
  }

  std::string_view getField() {
    qdb_assert(keyType != KeyType::kParseError && keyType != KeyType::kString);
    return firstChunk.getRawSuffix();
  }

  std::string_view getRawPrefixUntilBoundary() {
    qdb_assert(keyType != KeyType::kParseError && keyType != KeyType::kString);
    return std::string_view(slice.data(), firstChunk.getBoundary()+1);
  }

  bool isLocalityIndex() {
    if(keyType != KeyType::kLocalityHash) return false;

    std::string_view field = getField();
    qdb_assert(field.size() != 0u);
    return *(field.data()) == char(InternalLocalityFieldType::kIndex);
  }

private:
  std::string_view slice;

  KeyType keyType = KeyType::kParseError;
  EscapedPrefixExtractor firstChunk;
};

}

#endif
