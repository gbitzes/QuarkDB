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

#ifndef __QUARKDB_KEY_LOCATORS_H__
#define __QUARKDB_KEY_LOCATORS_H__

#include "KeyDescriptor.hh"
#include "../utils/SmartBuffer.hh"
#include "../utils/StringUtils.hh"

namespace quarkdb {

using KeyBuffer = SmartBuffer<30>;

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
    keyBuffer.resize(1 + redisKey.size() + StringUtils::countOccurences(redisKey, '#') + 1);

    keyBuffer[0] = char(keyType);

    size_t targetIndex = 1;
    for(size_t i = 0; i < redisKey.size(); i++) {
      if(redisKey[i] != '#') {
        keyBuffer[targetIndex] = redisKey[i];
        targetIndex++;
      }
      else {
        keyBuffer[targetIndex] = '|';
        keyBuffer[targetIndex+1] = '#';

        targetIndex += 2;
      }
    }

    keyBuffer[targetIndex] = '#';
    keyPrefixSize = targetIndex + 1;
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

}

#endif
