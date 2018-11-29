// ----------------------------------------------------------------------
// File: PinnedBuffer.hh
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

#ifndef QUARKDB_PINNED_BUFFER_HH
#define QUARKDB_PINNED_BUFFER_HH

#include <memory>
#include <string_view>

namespace quarkdb {

class MemoryRegion;

//------------------------------------------------------------------------------
// This is a buffer "pinned" to a MemoryRegion. As long as such an object is
// alive, it keeps a reference to its corresponding MemoryRegion.
//
// This way, it's possible to tell if any given MemoryRegion has any active
// buffers depending on it, and ensures the underlying MemoryRegion will not
// be de-allocated from under our feet.
//
// It's also possible to have this object own its buffer, as an internal
// std::string.
//
// NOTE: While we allow changing the contents of the buffer, the size is
//       immutable. A new object needs to be created if you want to change the
//       size.
//------------------------------------------------------------------------------
class PinnedBuffer {
public:
  //----------------------------------------------------------------------------
  // Constructor: Make empty PinnedBuffer.
  //----------------------------------------------------------------------------
  PinnedBuffer() {}

  //----------------------------------------------------------------------------
  // Constructor: Pass the corresponding reference to MemoryRegion, as well as
  // the chunk we're pointing to.
  //----------------------------------------------------------------------------
  PinnedBuffer(std::shared_ptr<MemoryRegion> ref, char* rgptr, size_t rgsz)
  : region(ref), regionPtr(rgptr), regionSize(rgsz) {}

  //----------------------------------------------------------------------------
  // Constructor: Use the internal buffer, allocate N bytes
  //----------------------------------------------------------------------------
  PinnedBuffer(size_t n) {
    internalBuffer.resize(n);
  }

  //----------------------------------------------------------------------------
  // Constructor: Use internal buffer, store given string_view. We do a deep
  // copy, this object may safely outlive the given contents.
  //----------------------------------------------------------------------------
  PinnedBuffer(std::string_view contents) : internalBuffer(contents) {}

  //----------------------------------------------------------------------------
  // Check if we're using internal storage or not
  //----------------------------------------------------------------------------
  bool usingInternalBuffer() const {
    return region == nullptr;
  }

  //----------------------------------------------------------------------------
  // Check size
  //----------------------------------------------------------------------------
  size_t size() const {
    if(region != nullptr) {
      return regionSize;
    }

    return internalBuffer.size();
  }

  //----------------------------------------------------------------------------
  // Explicit conversion to std::string_view
  //----------------------------------------------------------------------------
  std::string_view sv() const noexcept {
    if(region != nullptr) {
      return std::string_view(regionPtr, regionSize);
    }

    return internalBuffer;
  }

  //----------------------------------------------------------------------------
  // Implicit conversion to std::string_view
  //----------------------------------------------------------------------------
  operator std::string_view() const noexcept {
    return this->sv();
  }

  //----------------------------------------------------------------------------
  // Return reference to data
  //----------------------------------------------------------------------------
  char* data() {
    if(region) {
      return regionPtr;
    }

    return internalBuffer.data();
  }

  //----------------------------------------------------------------------------
  // Return const reference to data
  //----------------------------------------------------------------------------
  const char* data() const {
    if(region) {
      return regionPtr;
    }

    return internalBuffer.data();
  }

  //----------------------------------------------------------------------------
  // Allow access and modification of contents through brackets
  //----------------------------------------------------------------------------
  char& operator[](size_t i) {
    return data()[i];
  }

  //----------------------------------------------------------------------------
  // Return internal buffer
  //----------------------------------------------------------------------------
  std::string& getInternalBuffer() {
    return internalBuffer;
  }

  //----------------------------------------------------------------------------
  // Equality operator with std::string_view
  //----------------------------------------------------------------------------
  bool operator==(std::string_view sv) const {
    return std::string_view(*this) == sv;
  }

  //----------------------------------------------------------------------------
  // Equality operator - other PinnedBuffers
  //----------------------------------------------------------------------------
  bool operator==(const PinnedBuffer &other) const {
    return std::string_view(*this) == std::string_view(other);
  }

  //----------------------------------------------------------------------------
  // Drop last n characters
  //----------------------------------------------------------------------------
  void remove_suffix(size_t n) {
    if(region) {
      regionSize -= n;
      return;
    }

    internalBuffer.erase(internalBuffer.begin()+internalBuffer.size()-n,
      internalBuffer.end());
  }


private:
  std::shared_ptr<MemoryRegion> region;
  char* regionPtr = nullptr;
  size_t regionSize = 0u;
  std::string internalBuffer;
};

}

#endif
