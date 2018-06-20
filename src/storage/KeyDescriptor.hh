// ----------------------------------------------------------------------
// File: KeyDescriptor.hh
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

#ifndef QUARKDB_KEY_DESCRIPTOR_H
#define QUARKDB_KEY_DESCRIPTOR_H

#include "../utils/IntToBinaryString.hh"
#include "../utils/StaticBuffer.hh"
#include "../Utils.hh"

namespace quarkdb {

// Types of redis keys supported
enum class KeyType : char {
  kNull = '\0',
  kParseError = '=',
  kString = 'a',
  kHash = 'b',
  kSet = 'c',
  kList = 'd',
  kLocalityHash = 'e',
  kLease = 'f'
};

inline KeyType parseKeyType(char c) {
  switch(c) {
    case char(KeyType::kString): {
      return KeyType::kString;
    }
    case char(KeyType::kHash): {
      return KeyType::kHash;
    }
    case char(KeyType::kSet): {
      return KeyType::kSet;
    }
    case char(KeyType::kList): {
      return KeyType::kList;
    }
    case char(KeyType::kLocalityHash): {
      return KeyType::kLocalityHash;
    }
    default: {
      return KeyType::kParseError;
    }
  }
}

// Helper enum for selecting which of startIndex, endIndex to pick
enum class Direction : int {
  kLeft = -1,
  kRight = 1
};

inline Direction flipDirection(Direction direction) {
  if(direction == Direction::kLeft) {
    return Direction::kRight;
  }
  else if(direction == Direction::kRight) {
    return Direction::kLeft;
  }
  qdb_throw("should never happen");
}

// Class which parses and serializes key descriptors.
class KeyDescriptor {
public:
  KeyDescriptor() {}
  KeyDescriptor(const std::string& str) {
    qdb_assert(str.size() != 0);

    // Determine key type.
    keyType = parseKeyType(str[0]);

    // Parse further
    switch(keyType) {
      case KeyType::kString:
      case KeyType::kSet:
      case KeyType::kHash:
      case KeyType::kLocalityHash: {
        qdb_assert(str.size() == kHashDescriptorSize);

        // Parse size.
        sz = binaryStringToInt(str.data() + kOffsetSize);

        // All done
        return;
      }
      case KeyType::kList: {
        qdb_assert(str.size() == kListDescriptorSize);

        // Parse size.
        sz = binaryStringToInt(str.data() + kOffsetSize);

        // Parse startIndex and endIndex.
        startIndex = binaryStringToInt(str.data() + kOffsetStartIndex);
        endIndex = binaryStringToInt(str.data() + kOffsetEndIndex);
        qdb_assert(startIndex <= endIndex);

        // All done
        return;
      }
      default: {
        qdb_throw("error parsing key descriptor");
      }
    }
  }

  bool empty() const {
    return keyType == KeyType::kNull;
  }

  KeyType getKeyType() const {
    return keyType;
  }

  int64_t getSize() const {
    qdb_assert(keyType != KeyType::kParseError);
    return sz;
  }

  uint64_t getStartIndex() const {
    qdb_assert(keyType == KeyType::kList);
    return startIndex;
  }

  uint64_t getEndIndex() const {
    qdb_assert(keyType == KeyType::kList);
    return endIndex;
  }

  void setKeyType(KeyType kt) {
    keyType = kt;
  }

  void setSize(int64_t newval) {
    qdb_assert(keyType != KeyType::kParseError && keyType != KeyType::kNull);
    sz = newval;
  }

  void setStartIndex(uint64_t newval) {
    qdb_assert(keyType == KeyType::kList);
    startIndex = newval;
  }

  void setEndIndex(uint64_t newval) {
    qdb_assert(keyType == KeyType::kList);
    endIndex = newval;
  }

  rocksdb::Slice serialize() {
    serializationBuffer[0] = char(keyType);

    switch(keyType) {
      case KeyType::kString:
      case KeyType::kSet:
      case KeyType::kHash:
      case KeyType::kLocalityHash: {
        serializationBuffer.shrink(kHashDescriptorSize);

        // Store the size..
        intToBinaryString(sz, serializationBuffer.data() + kOffsetSize);
        return serializationBuffer.toSlice();
      }
      case KeyType::kList: {
        serializationBuffer.shrink(kListDescriptorSize);

        // Store the size..
        intToBinaryString(sz, serializationBuffer.data() + kOffsetSize);

        // Store start index, end index
        intToBinaryString(startIndex, serializationBuffer.data() + kOffsetStartIndex);
        intToBinaryString(endIndex, serializationBuffer.data() + kOffsetEndIndex);

        qdb_assert(startIndex <= endIndex);
        return serializationBuffer.toSlice();
      }
      default: {
        qdb_throw("attempted to serialize invalid key descriptor");
      }
    }
  }

  bool operator==(const KeyDescriptor &rhs) const {
    return keyType == rhs.keyType && sz == rhs.sz &&
           startIndex == rhs.startIndex && endIndex == rhs.endIndex;
  }

  uint64_t getListIndex(Direction direction) {
    qdb_assert(keyType == KeyType::kList);
    if(direction == Direction::kLeft) {
      return startIndex;
    }
    else if(direction == Direction::kRight) {
      return endIndex;
    }
    qdb_throw("should never happen");
  }

  void setListIndex(Direction direction, uint64_t newindex) {
    qdb_assert(keyType == KeyType::kList);
    if(direction == Direction::kLeft) {
      startIndex = newindex;
      return;
    }
    else if(direction == Direction::kRight) {
      endIndex = newindex;
      return;
    }
    qdb_throw("should never happen");
  }

private:
  KeyType keyType = KeyType::kNull;

  static constexpr size_t kMaxDescriptorSize = 28;
  StaticBuffer<kMaxDescriptorSize> serializationBuffer;

  static constexpr size_t kStringDescriptorSize = 1 + sizeof(int64_t);
  static constexpr size_t kHashDescriptorSize = 1 + sizeof(int64_t);
  static constexpr size_t kListDescriptorSize = 1 + sizeof(int64_t) + 2*sizeof(uint64_t);

  static constexpr size_t kOffsetSize = 1;
  static constexpr size_t kOffsetStartIndex = 1 + sizeof(int64_t);
  static constexpr size_t kOffsetEndIndex = 1 + sizeof(int64_t) + sizeof(uint64_t);

  // Only used in hashes, sets, and lists
  int64_t sz = 0;

  // Only used in lists
  static constexpr uint64_t kIndexInitialValue = std::numeric_limits<uint64_t>::max() / 2;
  uint64_t startIndex = kIndexInitialValue;
  uint64_t endIndex = kIndexInitialValue;
};

}

#endif
