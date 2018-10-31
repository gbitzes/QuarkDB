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
using ClockValue = uint64_t;

enum class InternalKeyType : char {
  kInternal = '_',
  kConfiguration = '~',
  kDescriptor = '!',
  kExpirationEvent = '@'
};

class DescriptorLocator {
public:
  DescriptorLocator() {}

  DescriptorLocator(std::string_view redisKey) {
    reset(redisKey);
  }

  void reset(std::string_view redisKey) {
    keyBuffer.resize(redisKey.size() + 1);
    keyBuffer[0] = char(InternalKeyType::kDescriptor);
    memcpy(keyBuffer.data()+1, redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }

  std::string toString() {
    return keyBuffer.toString();
  }

private:
  KeyBuffer keyBuffer;
};

class StringLocator {
public:
  StringLocator(std::string_view redisKey) {
    reset(redisKey);
  }

  void reset(std::string_view redisKey) {
    keyBuffer.resize(redisKey.size() + 1);
    keyBuffer[0] = char(KeyType::kString);
    memcpy(keyBuffer.data()+1, redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }

private:
  KeyBuffer keyBuffer;
};

// Append hash-escaped string into KeyBuffer. Assumption: KeyBuffer has enough
// space!
QDB_ALWAYS_INLINE
inline size_t appendEscapedString(KeyBuffer &keyBuffer, size_t targetIndex, std::string_view str) {
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
  FieldLocator(const KeyType &keyType, std::string_view redisKey) {
    resetKey(keyType, redisKey);
  }

  FieldLocator(const KeyType &keyType, std::string_view redisKey, std::string_view field) {
    resetKey(keyType, redisKey);
    resetField(field);
  }

  void resetKey(const KeyType &keyType, std::string_view redisKey) {
    qdb_assert(keyType == KeyType::kHash || keyType == KeyType::kSet || keyType == KeyType::kDeque);
    keyBuffer.resize(1 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 2);

    keyBuffer[0] = char(keyType);
    keyPrefixSize = appendEscapedString(keyBuffer, 1, redisKey);
  }

  void resetField(std::string_view field) {
    keyBuffer.shrink(keyPrefixSize);
    keyBuffer.expand(keyPrefixSize + field.size());
    memcpy(keyBuffer.data() + keyPrefixSize, field.data(), field.size());
  }

  std::string_view getPrefix() {
    return std::string_view(keyBuffer.data(), keyPrefixSize);
  }

  rocksdb::Slice getPrefixSlice() {
    return rocksdb::Slice(keyBuffer.data(), keyPrefixSize);
  }

  size_t getPrefixSize() {
    return keyPrefixSize;
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
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
  LocalityFieldLocator(std::string_view redisKey, std::string_view hint, std::string_view field) {
    resetKey(redisKey);
    resetHint(hint);
    resetField(field);
  }

  LocalityFieldLocator(std::string_view redisKey, std::string_view hint) {
    resetKey(redisKey);
    resetHint(hint);
  }

  LocalityFieldLocator(std::string_view redisKey) {
    resetKey(redisKey);
  }

  void resetKey(std::string_view redisKey) {
    qdb_assert(!redisKey.empty());

    keyBuffer.resize(2 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 2);
    keyBuffer[0] = char(KeyType::kLocalityHash);
    keyPrefixSize = appendEscapedString(keyBuffer, 1, redisKey);
    keyBuffer[keyPrefixSize++] = char(InternalLocalityFieldType::kData);

    localityPrefixSize = 0;
  }

  void resetHint(std::string_view hint) {
    qdb_assert(!hint.empty());
    qdb_assert(keyPrefixSize != 0);

    keyBuffer.shrink(keyPrefixSize);
    keyBuffer.expand(keyPrefixSize + hint.size() + StringUtils::countOccurences(hint, '#') + 2);
    localityPrefixSize = appendEscapedString(keyBuffer, keyPrefixSize, hint);
  }

  void resetField(std::string_view field) {
    qdb_assert(!field.empty());
    qdb_assert(localityPrefixSize != 0);

    keyBuffer.shrink(localityPrefixSize);
    keyBuffer.expand(localityPrefixSize + field.size());
    memcpy(keyBuffer.data() + localityPrefixSize, field.data(), field.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }

  std::string_view getPrefix() {
    return std::string_view(keyBuffer.data(), keyPrefixSize + localityPrefixSize);
  }

private:
  size_t keyPrefixSize = 0;
  size_t localityPrefixSize = 0;
  KeyBuffer keyBuffer;
};

class LocalityIndexLocator {
public:
  LocalityIndexLocator(std::string_view redisKey, std::string_view field) {
    resetKey(redisKey);
    resetField(field);
  }

  LocalityIndexLocator(std::string_view redisKey) {
    resetKey(redisKey);
  }

  void resetKey(std::string_view redisKey) {
    qdb_assert(!redisKey.empty());

    keyBuffer.resize(2 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 2);
    keyBuffer[0] = char(KeyType::kLocalityHash);
    keyPrefixSize = appendEscapedString(keyBuffer, 1, redisKey);
    keyBuffer[keyPrefixSize++] = char(InternalLocalityFieldType::kIndex);
  }

  void resetField(std::string_view field) {
    qdb_assert(!field.empty());

    keyBuffer.shrink(keyPrefixSize);
    keyBuffer.expand(keyPrefixSize + field.size());
    memcpy(keyBuffer.data() + keyPrefixSize, field.data(), field.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }

private:
  size_t keyPrefixSize = 0;
  KeyBuffer keyBuffer;
};

class LeaseLocator {
public:
  LeaseLocator(std::string_view redisKey) {
    reset(redisKey);
  }

  void reset(std::string_view redisKey) {
    keyBuffer.resize(redisKey.size() + 1);
    keyBuffer[0] = char(KeyType::kLease);
    memcpy(keyBuffer.data()+1, redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }
private:
  KeyBuffer keyBuffer;
};

class ExpirationEventLocator {
public:
  ExpirationEventLocator(ClockValue deadline, std::string_view redisKey) {
    reset(deadline, redisKey);
  }

  void reset(ClockValue deadline, std::string_view redisKey) {
    keyBuffer.resize(1 + sizeof(ClockValue) + redisKey.size());
    keyBuffer[0] = char(InternalKeyType::kExpirationEvent);

    unsignedIntToBinaryString(deadline, keyBuffer.data() + 1);
    memcpy(keyBuffer.data()+1+sizeof(ClockValue), redisKey.data(), redisKey.size());
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }

private:
  KeyBuffer keyBuffer;
};

class ConfigurationLocator {
public:
  ConfigurationLocator(std::string_view key) {
    reset(key);
  }

  void reset(std::string_view key) {
    keyBuffer.resize(1 + key.size());
    keyBuffer[0] = char(InternalKeyType::kConfiguration);

    memcpy(keyBuffer.data()+1, key.data(), key.size());
  }

  std::string_view toView() {
    return keyBuffer.toView();
  }

  rocksdb::Slice toSlice() {
    return keyBuffer.toSlice();
  }

private:
  KeyBuffer keyBuffer;
};

}

#endif
