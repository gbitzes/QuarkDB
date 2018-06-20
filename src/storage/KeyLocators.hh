// ----------------------------------------------------------------------
// File: KeyLocators.hh
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

#ifndef QUARKDB_KEY_LOCATORS_H
#define QUARKDB_KEY_LOCATORS_H

#include "KeyDescriptor.hh"
#include "../utils/SmartBuffer.hh"
#include "../utils/StringUtils.hh"

namespace quarkdb {

using KeyBuffer = SmartBuffer<512>;

enum class InternalKeyType : char {
  kInternal = '_',
  kConfiguration = '~',
  kDescriptor = '!'
};

class DescriptorLocator {
public:
  DescriptorLocator() {}

  DescriptorLocator(const std::string &redisKey) {
    reset(redisKey);
  }

  void reset(const std::string &redisKey) {
    keyBuffer.resize(redisKey.size() + 1);
    keyBuffer[0] = char(InternalKeyType::kDescriptor);
    memcpy(keyBuffer.data()+1, redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string toString() {
    return keyBuffer.toString();
  }

private:
  KeyBuffer keyBuffer;
};

class StringLocator {
public:
  StringLocator(const std::string &redisKey) {
    reset(redisKey);
  }

  void reset(const std::string &redisKey) {
    keyBuffer.resize(redisKey.size() + 1);
    keyBuffer[0] = char(KeyType::kString);
    memcpy(keyBuffer.data()+1, redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }
private:
  KeyBuffer keyBuffer;
};

// Append hash-escaped string into KeyBuffer. Assumption: KeyBuffer has enough
// space!
QDB_ALWAYS_INLINE
inline size_t appendEscapedString(KeyBuffer &keyBuffer, size_t targetIndex, const std::string &str) {
  for(size_t i = 0; i < str.size(); i++) {
    if(str[i] != '#') {
      keyBuffer[targetIndex] = str[i];
      targetIndex++;
    }
    else {
      keyBuffer[targetIndex] = '|';
      keyBuffer[targetIndex+1] = '#';

      targetIndex += 2;
    }
  }

  keyBuffer[targetIndex] = '#';
  keyBuffer[targetIndex+1] = '#';
  return targetIndex+2;
}

class FieldLocator {
public:
  FieldLocator(const KeyType &keyType, const std::string &redisKey) {
    resetKey(keyType, redisKey);
  }

  FieldLocator(const KeyType &keyType, const std::string &redisKey, const std::string &field) {
    resetKey(keyType, redisKey);
    resetField(field);
  }

  void resetKey(const KeyType &keyType, const std::string &redisKey) {
    qdb_assert(keyType == KeyType::kHash || keyType == KeyType::kSet || keyType == KeyType::kList);
    keyBuffer.resize(1 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 2);

    keyBuffer[0] = char(keyType);
    keyPrefixSize = appendEscapedString(keyBuffer, 1, redisKey);
  }

  void resetField(const std::string &field) {
    keyBuffer.shrink(keyPrefixSize);
    keyBuffer.expand(keyPrefixSize + field.size());
    memcpy(keyBuffer.data() + keyPrefixSize, field.data(), field.size());
  }

  rocksdb::Slice getPrefix() {
    return rocksdb::Slice(keyBuffer.data(), keyPrefixSize);
  }

  size_t getPrefixSize() {
    return keyPrefixSize;
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

private:
  size_t keyPrefixSize = 0;
  KeyBuffer keyBuffer;
};

enum class InternalLocalityFieldType : char {
  kData = 'd',
  kIndex = 'i'
};

class LocalityFieldLocator {
public:
  LocalityFieldLocator(const std::string &redisKey, const std::string &hint, const std::string &field) {
    resetKey(redisKey);
    resetHint(hint);
    resetField(field);
  }

  LocalityFieldLocator(const std::string &redisKey, const std::string &hint) {
    resetKey(redisKey);
    resetHint(hint);
  }

  LocalityFieldLocator(const std::string &redisKey) {
    resetKey(redisKey);
  }

  void resetKey(const std::string &redisKey) {
    qdb_assert(!redisKey.empty());

    keyBuffer.resize(2 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 2);
    keyBuffer[0] = char(KeyType::kLocalityHash);
    keyPrefixSize = appendEscapedString(keyBuffer, 1, redisKey);
    keyBuffer[keyPrefixSize++] = char(InternalLocalityFieldType::kData);

    localityPrefixSize = 0;
  }

  void resetHint(const std::string &hint) {
    qdb_assert(!hint.empty());
    qdb_assert(keyPrefixSize != 0);

    keyBuffer.shrink(keyPrefixSize);
    keyBuffer.expand(keyPrefixSize + hint.size() + StringUtils::countOccurences(hint, '#') + 2);
    localityPrefixSize = appendEscapedString(keyBuffer, keyPrefixSize, hint);
  }

  void resetField(const std::string &field) {
    qdb_assert(!field.empty());
    qdb_assert(localityPrefixSize != 0);

    keyBuffer.shrink(localityPrefixSize);
    keyBuffer.expand(localityPrefixSize + field.size());
    memcpy(keyBuffer.data() + localityPrefixSize, field.data(), field.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

private:
  size_t keyPrefixSize = 0;
  size_t localityPrefixSize = 0;
  KeyBuffer keyBuffer;
};

class LocalityIndexLocator {
public:
  LocalityIndexLocator(const std::string &redisKey, const std::string &field) {
    resetKey(redisKey);
    resetField(field);
  }

  LocalityIndexLocator(const std::string &redisKey) {
    resetKey(redisKey);
  }

  void resetKey(const std::string &redisKey) {
    qdb_assert(!redisKey.empty());

    keyBuffer.resize(2 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 2);
    keyBuffer[0] = char(KeyType::kLocalityHash);
    keyPrefixSize = appendEscapedString(keyBuffer, 1, redisKey);
    keyBuffer[keyPrefixSize++] = char(InternalLocalityFieldType::kIndex);
  }

  void resetField(const std::string &field) {
    qdb_assert(!field.empty());

    keyBuffer.shrink(keyPrefixSize);
    keyBuffer.expand(keyPrefixSize + field.size());
    memcpy(keyBuffer.data() + keyPrefixSize, field.data(), field.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

private:
  size_t keyPrefixSize = 0;
  KeyBuffer keyBuffer;
};

class LeaseLocator {
public:
  LeaseLocator(const std::string &redisKey) {
    reset(redisKey);
  }

  void reset(const std::string &redisKey) {
    keyBuffer.resize(redisKey.size() + 1);
    keyBuffer[0] = char(KeyType::kLease);
    memcpy(keyBuffer.data()+1, redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }
private:
  KeyBuffer keyBuffer;
};

}

#endif
