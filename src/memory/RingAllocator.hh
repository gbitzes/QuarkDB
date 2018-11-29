// ----------------------------------------------------------------------
// File: RingAllocator.hh
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

#ifndef QUARKDB_RING_ALLOCATOR_HH
#define QUARKDB_RING_ALLOCATOR_HH

#include "PinnedBuffer.hh"
#include <vector>
#include <cstddef>
#include <memory>
#include <optional>

namespace quarkdb {

//------------------------------------------------------------------------------
// A fancy way of saying "non-copyable contiguous array".
//------------------------------------------------------------------------------
class MemoryRegion : public std::enable_shared_from_this<MemoryRegion> {
public:
  //----------------------------------------------------------------------------
  // Construct
  //----------------------------------------------------------------------------
  static std::shared_ptr<MemoryRegion> Construct(size_t n) {
    return std::make_shared<MemoryRegion>(n);
  }

  //----------------------------------------------------------------------------
  // Private constructor - use "Construct" method which gives you a shared_ptr.
  // Don't use this! Should have been private, but make_shared doesn't seem
  // to work with private constructors.
  //----------------------------------------------------------------------------
  MemoryRegion(size_t n) {
    allocated = 0u;
    region.resize(n);
  }

  //----------------------------------------------------------------------------
  // NO moving: All referencing PinnedBuffers to the moved-from object would
  // become invalid..
  //----------------------------------------------------------------------------
  MemoryRegion& operator=(MemoryRegion&& other) = delete;
  MemoryRegion(MemoryRegion&& other) = delete;

  //----------------------------------------------------------------------------
  // No copy-constructor, no copy-assignment for obvious reasons
  //----------------------------------------------------------------------------
  MemoryRegion& operator=(const MemoryRegion& other) = delete;
  MemoryRegion(const MemoryRegion& other) = delete;

  //----------------------------------------------------------------------------
  // Allocate this amount of bytes, filling a PinnedBuffer.
  // Return false if we don't have enough space to service this request.
  //----------------------------------------------------------------------------
  std::optional<PinnedBuffer> allocate(size_t bytes) {
    if(allocated + bytes > region.size()) {
      return {};
    }

    char* ptr = (char*) (region.data() + allocated);
    allocated += bytes;
    return PinnedBuffer(shared_from_this(), ptr, bytes);
  }

  //----------------------------------------------------------------------------
  // Reset all allocations, and start from the beginning. Only call this
  // if you are certain there are no other references to this memory block.
  //----------------------------------------------------------------------------
  void resetAllocations() {
    allocated = 0u;
  }

  //----------------------------------------------------------------------------
  // Return size
  //----------------------------------------------------------------------------
  size_t size() const {
    return region.size();
  }

  //----------------------------------------------------------------------------
  // Return bytes consumed so far
  //----------------------------------------------------------------------------
  size_t bytesConsumed() const {
    return allocated;
  }

  //----------------------------------------------------------------------------
  // Return amount of free bytes
  //----------------------------------------------------------------------------
  size_t bytesFree() const {
    return size() - bytesConsumed();
  }

  //----------------------------------------------------------------------------
  // Return number of references to this object
  //----------------------------------------------------------------------------
  long refcount() const {
    return shared_from_this().use_count();
  }


private:
  std::vector<std::byte> region;
  size_t allocated = 0u;
};

//------------------------------------------------------------------------------
// There are certain memory allocation patterns which follow a queue-like
// behaviour: Requests on a single connection being a major example:
//
// - REQ1
// - REQ2
// - REQ3
// - REQ4
//
// Allocation pattern: REQ1, REQ2, REQ3, REQ4, ...
// De-allocation pattern: REQ1, REQ2, REQ3, REQ4, ...
//
// In such case, major performance gains can be realized by recycling
// the memory per-connection, and relieving a lot of pressure from the global
// memory allocator.
//
// Sized correctly, such an allocator can absorb virtually all hits to malloc
// that a single connection servicing requests would make. Moreover, each
// allocation request serviced by us will be far cheaper than malloc, as we're
// simply adjusting pointers, and not doing fully-general memory accounting.
//
// The cherry-on-top is that we benefit from cache locality, since requests
// will generally be accessed in the same order they are allocated.
//
// Do not use this unless the memory regions requested are (roughly)
// de-allocated in the same order they were allocated, as memory consumption
// will explode otherwise. It should, however, still behave correctly even
// in such case.
//------------------------------------------------------------------------------
// class RingAllocator {
// public:
//   //----------------------------------------------------------------------------
//   // Provide the size of allocated memory blocks, and the number of blocks to
//   // keep around for recycling.
//   //
//   // If a request for memory exceeds this size, direct malloc is used.
//   //
//   // TODO
//   //----------------------------------------------------------------------------
//   RingAllocator(size_t blockSize, size_t blockCount);
// };

}

#endif

