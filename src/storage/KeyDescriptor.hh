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

#ifndef QUARKDB_KEY_DESCRIPTOR_HH
#define QUARKDB_KEY_DESCRIPTOR_HH

#include "utils/IntToBinaryString.hh"
#include "utils/StaticBuffer.hh"
#include "Utils.hh"

namespace quarkdb {

// Types of redis keys supported
enum class KeyType : char {
  kNull = '\0',
  kParseError = '=',
  kString = 'a',
  kHash = 'b',
  kSet = 'c',
  kDeque = 'd',
  kLocalityHash = 'e',
  kLease = 'f',
  kVersionedHash = 'g'
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
    case char(KeyType::kDeque): {
      return KeyType::kDeque;
    }
    case char(KeyType::kLocalityHash): {
      return KeyType::kLocalityHash;
    }
    case char(KeyType::kLease): {
      return KeyType::kLease;
    }
    case char(KeyType::kVersionedHash): {
      return KeyType::kVersionedHash;
    }
    default: {
      return KeyType::kParseError;
    }
  }
}

inline std::string keyTypeAsString(KeyType key) {
  switch(key) {
    case KeyType::kNull: {
      return "none";
    }
    case KeyType::kParseError: {
      qdb_throw("given KeyType == kParseError, not representable as string");
    }
    case KeyType::kString: {
      return "string";
    }
    case KeyType::kSet: {
      return "set";
    }
    case KeyType::kHash: {
      return "hash";
    }
    case KeyType::kDeque: {
      return "deque";
    }
    case KeyType::kLocalityHash: {
      return "locality hash";
    }
    case KeyType::kLease: {
      return "lease";
    }
    case KeyType::kVersionedHash: {
      return "versioned hash";
    }
  }

  qdb_throw("should never reach here");
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
  KeyDescriptor(std::string_view str) {
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
      case KeyType::kDeque:
      case KeyType::kLease: {
        qdb_assert(str.size() == kDequeDescriptorSize);

        // Parse size.
        sz = binaryStringToInt(str.data() + kOffsetSize);

        // Parse startIndex and endIndex.
        startIndex = binaryStringToInt(str.data() + kOffsetStartIndex);
        endIndex = binaryStringToInt(str.data() + kOffsetEndIndex);
        qdb_assert(startIndex <= endIndex);

        // All done
        return;
      }
      case KeyType::kVersionedHash: {
        qdb_assert(str.size() == kVersionedHashDescriptorSize);

        // Parse size.
        sz = binaryStringToInt(str.data() + kOffsetSize);

        // Parse version, store into startIndex.
        startIndex = binaryStringToInt(str.data() + kOffsetStartIndex);

        // All done
        return;
      }
      default: {
        qdb_throw("error parsing key descriptor - unknown key type");
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
    qdb_assert(keyType == KeyType::kDeque || keyType == KeyType::kLease || keyType == KeyType::kVersionedHash);
    return startIndex;
  }

  uint64_t getEndIndex() const {
    qdb_assert(keyType == KeyType::kDeque || keyType == KeyType::kLease);
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
    qdb_assert(keyType == KeyType::kDeque || keyType == KeyType::kLease || keyType == KeyType::kVersionedHash);
    startIndex = newval;
  }

  void setEndIndex(uint64_t newval) {
    qdb_assert(keyType == KeyType::kDeque || keyType == KeyType::kLease);
    endIndex = newval;
  }

  std::string_view serialize() {
    serializationBuffer[0] = char(keyType);

    switch(keyType) {
      case KeyType::kString:
      case KeyType::kSet:
      case KeyType::kHash:
      case KeyType::kLocalityHash: {
        serializationBuffer.shrink(kHashDescriptorSize);

        // Store the size..
        intToBinaryString(sz, serializationBuffer.data() + kOffsetSize);
        return serializationBuffer.toView();
      }
      case KeyType::kDeque:
      case KeyType::kLease: {
        serializationBuffer.shrink(kDequeDescriptorSize);

        // Store the size..
        intToBinaryString(sz, serializationBuffer.data() + kOffsetSize);

        // Store start index, end index
        intToBinaryString(startIndex, serializationBuffer.data() + kOffsetStartIndex);
        intToBinaryString(endIndex, serializationBuffer.data() + kOffsetEndIndex);

        qdb_assert(startIndex <= endIndex);
        return serializationBuffer.toView();
      }
      case KeyType::kVersionedHash: {
        serializationBuffer.shrink(kVersionedHashDescriptorSize);

        // Store the size..
        intToBinaryString(sz, serializationBuffer.data() + kOffsetSize);

        // Store start index
        intToBinaryString(startIndex, serializationBuffer.data() + kOffsetStartIndex);
        return serializationBuffer.toView();
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
    qdb_assert(keyType == KeyType::kDeque);
    if(direction == Direction::kLeft) {
      return startIndex;
    }
    else if(direction == Direction::kRight) {
      return endIndex;
    }
    qdb_throw("should never happen");
  }

  void setListIndex(Direction direction, uint64_t newindex) {
    qdb_assert(keyType == KeyType::kDeque);
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
  static constexpr size_t kDequeDescriptorSize = 1 + sizeof(int64_t) + 2*sizeof(uint64_t);
  static constexpr size_t kVersionedHashDescriptorSize = 1 + sizeof(int64_t) + 1*sizeof(uint64_t);

  static constexpr size_t kOffsetSize = 1;
  static constexpr size_t kOffsetStartIndex = 1 + sizeof(int64_t);
  static constexpr size_t kOffsetEndIndex = 1 + sizeof(int64_t) + sizeof(uint64_t);

  // Only used in hashes, sets, and deques
  int64_t sz = 0;

  // Only used in deques. startIndex is also used in versioned hashes.
  static constexpr uint64_t kIndexInitialValue = std::numeric_limits<uint64_t>::max() / 2;
  uint64_t startIndex = kIndexInitialValue;
  uint64_t endIndex = kIndexInitialValue;
};

}

#endif
