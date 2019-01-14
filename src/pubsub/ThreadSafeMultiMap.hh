// ----------------------------------------------------------------------
// File: ThreadSafeMultiMap.hh
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

#ifndef QUARKDB_PUBSUB_THREAD_SAFE_MULTIMAP_HH
#define QUARKDB_PUBSUB_THREAD_SAFE_MULTIMAP_HH

#include <shared_mutex>
#include <map>
#include <set>
#include <list>

namespace quarkdb {

//------------------------------------------------------------------------------
// A thread-safe multi-map used for tracking pub-sub subscriptions.
//
// Modifying the map while iterators are held by other threasds is safe, too.
// Only items present in the map during the entire duration of iteration are
// guaranteed to be returned - other elements, which are inserted or deleted
// while a particular iteration is ongoing may or may not be included in the
// results.
//------------------------------------------------------------------------------
template<typename Key, typename Value>
class ThreadSafeMultiMap {
public:

  //----------------------------------------------------------------------------
  // Insert the given key and value - return false if it existed already.
  //----------------------------------------------------------------------------
  bool insert(const Key &key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mtx);
    auto match = contents[key].emplace(value);
    storedEntries += match.second;
    return match.second;
  }

  //----------------------------------------------------------------------------
  // Erase the given key-value pair - return false if it didn't exist.
  //----------------------------------------------------------------------------
  bool erase(const Key &key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mtx);

    auto targetSet = contents.find(key);
    if(targetSet == contents.end()) {
      return false;
    }

    auto match = targetSet->second.erase(value);
    storedEntries -= match;

    if(targetSet->second.empty()) {
      contents.erase(key);
    }

    return match;
  }

  //----------------------------------------------------------------------------
  // Get total number of entries stored
  //----------------------------------------------------------------------------
  size_t size() const {
    std::shared_lock<std::shared_mutex> lock(mtx);
    return storedEntries;
  }

  //----------------------------------------------------------------------------
  // Key iterator: Iterates *only* through the keys of this map, ignoring the
  // values.
  //----------------------------------------------------------------------------
  class KeyIterator {
  public:
    //--------------------------------------------------------------------------
    // Empty constructor
    //--------------------------------------------------------------------------
    KeyIterator() {
      isValid = false;
    }

    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    KeyIterator(ThreadSafeMultiMap<Key, Value> *trg, size_t stage)
    : target(trg), stageSize(stage) {
      populateStage(nullptr);
    }

    //--------------------------------------------------------------------------
    // Check if iterator is vald
    //--------------------------------------------------------------------------
    bool valid() const {
      return isValid;
    }

    //--------------------------------------------------------------------------
    // Advance iterator
    //--------------------------------------------------------------------------
    void next() {
      if(stage.size() == 1u) {
        populateStage(&stage.front());
        stage.pop_front();
        return;
      }

      stage.pop_front();
    }

    //--------------------------------------------------------------------------
    // Get key we're pointing to
    //--------------------------------------------------------------------------
    Key getKey() const {
      return stage.front();
    }

  private:
    //--------------------------------------------------------------------------
    // Populate the stage of elements to serve until next lock acquisition
    //--------------------------------------------------------------------------
    void populateStage(const Key* lastKey) {
      std::shared_lock<std::shared_mutex> lock(target->mtx);

      typename std::map<Key, std::set<Value>>::iterator it;
      if(lastKey) {
        it = target->contents.upper_bound(*lastKey);
      }
      else {
        it = target->contents.begin();
      }

      if(it == target->contents.end()) {
        isValid = false;
        return;
      }

      for(size_t i = 0; i < stageSize; i++) {
        if(it == target->contents.end()) {
          return;
        }


        stage.push_back(it->first);
        it++;
      }
    }

    ThreadSafeMultiMap<Key, Value> *target;
    size_t stageSize;

    std::list<Key> stage;
    bool isValid = true;
  };

  //----------------------------------------------------------------------------
  // Match iterator: Iterate through values which match the given Key.
  //----------------------------------------------------------------------------
  class MatchIterator {
  public:
    //--------------------------------------------------------------------------
    // Empty constructor
    //--------------------------------------------------------------------------
    MatchIterator() {
      isValid = false;
    }

    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    MatchIterator(ThreadSafeMultiMap<Key, Value> *trg, const Key& k, size_t
      st) : target(trg), targetKey(k), stageSize(st) {

      populateStage(nullptr);
    }

    //--------------------------------------------------------------------------
    // Check if iterator is vald
    //--------------------------------------------------------------------------
    bool valid() const {
      return isValid;
    }

    //--------------------------------------------------------------------------
    // Erase element
    //--------------------------------------------------------------------------
    bool erase() {
      return target->erase(targetKey, stage.front());
    }

    //--------------------------------------------------------------------------
    // Advance iterator
    //--------------------------------------------------------------------------
    void next() {
      if(stage.size() == 1u) {
        populateStage(&stage.front());
        stage.pop_front();
        return;
      }

      stage.pop_front();
    }

    //--------------------------------------------------------------------------
    // Get value we're pointing to
    //--------------------------------------------------------------------------
    Value getValue() const {
      return stage.front();
    }

    //--------------------------------------------------------------------------
    // Get key we're pointing to
    //--------------------------------------------------------------------------
    Key getKey() const {
      return targetKey;
    }

  private:
    //--------------------------------------------------------------------------
    // Populate the stage of elements to serve until next lock acquisition
    //--------------------------------------------------------------------------
    void populateStage(const Value* lastValue) {
      std::shared_lock<std::shared_mutex> lock(target->mtx);

      auto firstIterator = target->contents.find(targetKey);
      if(firstIterator == target->contents.end()) {
        isValid = false;
        return;
      }

      typename std::set<Value>::iterator it;
      if(lastValue) {
        it = firstIterator->second.upper_bound(*lastValue);
      }
      else {
        it = firstIterator->second.begin();
      }

      if(it == firstIterator->second.end()) {
        isValid = false;
        return;
      }

      for(size_t i = 0; i < stageSize; i++) {
        if(it == firstIterator->second.end()) {
          return;
        }

        stage.push_back(*it);
        it++;
      }
    }

    ThreadSafeMultiMap<Key, Value> *target;
    Key targetKey;
    size_t stageSize;

    std::list<Value> stage;
    bool isValid = true;
  };

  //----------------------------------------------------------------------------
  // Retrieve a key iterator
  //----------------------------------------------------------------------------
  KeyIterator getKeyIterator(size_t stage = 100) {
    return KeyIterator(this, stage);
  }

  //----------------------------------------------------------------------------
  // Retrieve a match iterator
  //----------------------------------------------------------------------------
  MatchIterator findMatching(const Key& lookup, size_t stage = 100) {
    return MatchIterator(this, lookup, stage);
  }

private:
  mutable std::shared_mutex mtx;
  std::map<Key, std::set<Value>> contents;
  std::map<Value, std::set<Key>> reverseContents;
  size_t storedEntries = 0u;
};


}

#endif