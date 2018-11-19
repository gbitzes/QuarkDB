// ----------------------------------------------------------------------
// File: SmartBuffer.hh
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

#ifndef __QUARKDB_UTILS_SMART_BUFFER_H__
#define __QUARKDB_UTILS_SMART_BUFFER_H__

#include <rocksdb/slice.h>

namespace quarkdb {

//------------------------------------------------------------------------------
// A smart buffer which tries to allocate its storage inline, up to a
// maximum of StaticSize bytes. If requested size exceeds StaticSize, we
// grudgingly allocate a new buffer on the heap, and use that.
//------------------------------------------------------------------------------

template<int StaticSize>
class SmartBuffer {
public:

  SmartBuffer() {}

  SmartBuffer(size_t size) {
    resize(size);
  }

  void resize(size_t size) {
    if(containerSize() < size) {
      deallocate();
      allocate(size);
    }

    realSize = size;
  }

  // We always keep the old contents
  void shrink(size_t size) {
    qdb_assert(size <= realSize);
    realSize = size;
  }

  // We keep the old contents, even in the case of re-allocation
  void expand(size_t size) {
    qdb_assert(realSize <= size);

    if(size < containerSize()) {
      // Easy path, no copying necessary
      realSize = size;
      return;
    }

    // Must re-allocate, while keeping the old contents - tricky
    char* oldHeapBuffer = heapBuffer;
    allocate(size);

    if(oldHeapBuffer) {
      memcpy(data(), oldHeapBuffer, realSize);
      free(oldHeapBuffer);
    }
    else {
      memcpy(data(), staticBuffer, realSize);
    }

    realSize = size;
  }

  ~SmartBuffer() {
    deallocate();
  }

  char* data() {
    if(heapBuffer) {
      return heapBuffer;
    }

    return staticBuffer;
  }

  size_t size() const {
    return realSize;
  }

  std::string toString() {
    return std::string(data(), size());
  }

  std::string_view toView() {
    return std::string_view(data(), size());
  }

  char& operator[](size_t i) {
    return data()[i];
  }

private:
  char staticBuffer[StaticSize];
  size_t realSize = StaticSize;
  char* heapBuffer = nullptr;
  size_t heapBufferSize = 0;

  size_t containerSize() {
    if(heapBuffer) {
      return heapBufferSize;
    }

    return StaticSize;
  }

  void deallocate() {
    if(heapBuffer) {
      free(heapBuffer);
      heapBuffer = nullptr;
    }
  }

  void allocate(size_t size) {
    if(realSize < size) {
      heapBuffer = (char*) malloc(size * sizeof(char) );
      heapBufferSize = size;
    }
  }
};

}

#endif
