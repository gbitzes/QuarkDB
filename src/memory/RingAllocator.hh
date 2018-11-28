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

#include <vector>
#include <cstddef>
#include <memory>

namespace quarkdb {

//------------------------------------------------------------------------------
// A fancy way of saying "non-copyable contiguous array".
//------------------------------------------------------------------------------
class MemoryRegion {
public:
  //----------------------------------------------------------------------------
  // Empty constructor
  //----------------------------------------------------------------------------
  MemoryRegion() { }

  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  MemoryRegion(size_t n) {
    allocated = 0u;
    region.resize(n);
  }

  //----------------------------------------------------------------------------
  // Move-constructor
  //----------------------------------------------------------------------------
  MemoryRegion(MemoryRegion&& other) {
    region = std::move(other.region);
  }

  //----------------------------------------------------------------------------
  // NO move assignment: We could accidentally overwrite an existing object,
  // while there are dangling references to it.
  //----------------------------------------------------------------------------
  MemoryRegion& operator=(MemoryRegion&& other) = delete;

  //----------------------------------------------------------------------------
  // No copy-constructor, no copy-assignment for obvious reasons
  //----------------------------------------------------------------------------
  MemoryRegion& operator=(const MemoryRegion& other) = delete;
  MemoryRegion(const MemoryRegion& other) = delete;

  //----------------------------------------------------------------------------
  // Allocate this amount of bytes, returning pointer to memory region
  // in question.
  // Return nullptr if we don't have enough space to service this request.
  //----------------------------------------------------------------------------
  std::byte* allocate(size_t bytes) {
    if(allocated + bytes > region.size()) {
      return nullptr;
    }

    std::byte* retval = region.data() + allocated;
    allocated += bytes;
    return retval;
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
class RingAllocator {
public:
  //----------------------------------------------------------------------------
  // Provide the size of allocated memory blocks, and the number of blocks to
  // keep around for recycling.
  //
  // If a request for memory exceeds this size, direct malloc is used.
  //
  // TODO
  //----------------------------------------------------------------------------
  RingAllocator(size_t blockSize, size_t blockCount);
};

}

#endif

