// ----------------------------------------------------------------------
// File: CoreLocalArray.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QUARKDB_CORE_LOCAL_ARRAY_HH
#define QUARKDB_CORE_LOCAL_ARRAY_HH

#include <stdlib.h>
#include <utility>
#include <memory>
#include <sched.h>
#include <thread>

//------------------------------------------------------------------------------
// Implementation largely inspired from rocksdb/util/core_local.h
//
//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//------------------------------------------------------------------------------

namespace quarkdb {

namespace CoreLocal {
  // TODO: Replace with std::hardware_destructive_interference_size
  // once supported by the compiler.
  constexpr size_t kCacheLine = 64;
};

//------------------------------------------------------------------------------
// An array in which each CPU core "owns" one of the elements.
// T must be cache-line aligned to prevent false sharing during writes.
//------------------------------------------------------------------------------
template<typename T>
class CoreLocalArray {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  CoreLocalArray() : cpus(std::thread::hardware_concurrency()),
   array(nullptr, free) {

    static_assert(sizeof(T) % CoreLocal::kCacheLine == 0,
      "CoreLocalArray only makes sense for cache-line aligned types");

    array = {
      reinterpret_cast<T*>(aligned_alloc(CoreLocal::kCacheLine, cpus*sizeof(T))),
      std::free
    };

    // Default-construct elements
    for(size_t i = 0; i < cpus; i++) {
      new (array.get() + i) T();
    }
  }

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~CoreLocalArray() {
    // Call destructors
    for(size_t i = 0; i < cpus; i++) {
      (array.get())[i].~T();
    }
  }

  //----------------------------------------------------------------------------
  // Get size of the array
  //----------------------------------------------------------------------------
  size_t size() const {
    return cpus;
  }

  //----------------------------------------------------------------------------
  // Access i-th element of the array, no matter which core we're executing at
  //----------------------------------------------------------------------------
  T* accessAtCore(size_t index) {
    return array.get() + index;
  }

  //----------------------------------------------------------------------------
  // Access i-th element of the array, no matter which core we're executing at.
  // Const overload.
  //----------------------------------------------------------------------------
  const T* accessAtCore(size_t index) const {
    return array.get() + index;
  }

  //----------------------------------------------------------------------------
  // Return the core index we would have used when calling access()
  //----------------------------------------------------------------------------
  int getCoreIndex() const {
    int cpuno = sched_getcpu();
    if(cpuno < 0 || cpuno > (int) cpus) {
      cpuno = 0;
    }

    return cpuno;
  }

  //----------------------------------------------------------------------------
  // Access element specific to the core we're currently running at, and return
  // our core index.
  //----------------------------------------------------------------------------
  std::pair<T*, size_t> access() {
    int cpuno = getCoreIndex();
    return {accessAtCore(cpuno), cpuno};
  }

private:
  size_t cpus;
  std::unique_ptr<T, decltype(free)*> array;
};


}

#endif
