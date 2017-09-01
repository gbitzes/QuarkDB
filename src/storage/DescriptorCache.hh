// ----------------------------------------------------------------------
// File: DescriptorCache.hh
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

#ifndef __QUARKDB_DESCRIPTOR_CACHE_H__
#define __QUARKDB_DESCRIPTOR_CACHE_H__

#include <mutex>

namespace quarkdb {

// This cache is currently only used during a bulk load.
class DescriptorCache {
public:
  struct Item {
    std::string value;
    std::mutex mutex;
  };

  bool get(const rocksdb::Slice &key, std::string &value) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = contents.find(key.ToString());
    if(it == contents.end()) {
      return false;
    }

    value = it->second->value;
    // it->second->mutex.lock();
    return true;
  }

  void put(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    std::lock_guard<std::mutex> lock(mtx);
    Item *item = contents[key.ToString()];
    if(!item) {
      item = new Item();
      contents[key.ToString()] = item;
      item->value = value.ToString();
    }
    else {
      item->value = value.ToString();
      item->mutex.unlock();
    }
  }

  std::map<std::string, Item*>::const_iterator begin() const {
    return contents.begin();
  }

  std::map<std::string, Item*>::const_iterator end() const {
    return contents.end();
  }

private:
  std::mutex mtx;
  std::map<std::string, Item*> contents;
};


}

#endif
