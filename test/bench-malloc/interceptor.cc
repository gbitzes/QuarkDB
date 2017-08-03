// ----------------------------------------------------------------------
// File: interceptor.cc
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

#include <locale.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdio.h>
#include <atomic>

//------------------------------------------------------------------------------
// This is used to intercept malloc and friends to keep track of how many
// allocations have occurred so far.
//------------------------------------------------------------------------------

std::atomic<int64_t> allocationCount {0};
std::atomic<int64_t> freeCount {0};

typedef void* (*malloc_type)(size_t);
typedef void  (*free_type)(void*);

int64_t getAllocationCount() {
  return allocationCount;
}

int64_t getFreeCount() {
  return freeCount;
}

extern void* malloc(size_t size) {
  allocationCount++;
  malloc_type real_malloc = (malloc_type)dlsym(RTLD_NEXT, "malloc");
  return real_malloc(size);
}

extern void free(void* ptr) {
  freeCount++;
  free_type real_free = (free_type)dlsym(RTLD_NEXT, "free");
  return real_free(ptr);
}
