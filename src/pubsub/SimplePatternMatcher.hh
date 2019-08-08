// ----------------------------------------------------------------------
// File: SimplePatternMatcher.hh
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

#ifndef QUARKDB_PUBSUB_SIMPLE_PATTERN_MATCHER_HH
#define QUARKDB_PUBSUB_SIMPLE_PATTERN_MATCHER_HH

#include "../../deps/StringMatchLen.h"
#include "pubsub/ThreadSafeMultiMap.hh"
#include <map>
#include <set>

namespace quarkdb {

//------------------------------------------------------------------------------
// This data structure allows two operations:
// - insert a pattern, with a value
// - check a value against which patterns it matches to
//
// Not thread-safe. Also, not safe to modify this structure while holding an
// iterator.
//
// Matching is not particularly efficient, as we scan through the entire
// contents to find a match.
//------------------------------------------------------------------------------
template<typename T>
class SimplePatternMatcher {
public:
  using Pattern = std::string;
  using Key = std::string;

  //----------------------------------------------------------------------------
  // Insert the given pattern and value.
  //----------------------------------------------------------------------------
  bool insert(const Pattern &pattern, const T& value) {
    return contents.insert(pattern, value);
  }

  //----------------------------------------------------------------------------
  // Erase the given pattern and value, if they exist
  //----------------------------------------------------------------------------
  bool erase(const Pattern &pattern, const T& value) {
    return contents.erase(pattern, value);
  }

  //----------------------------------------------------------------------------
  // Get total number of values stored
  //----------------------------------------------------------------------------
  size_t size() const {
    return contents.size();
  }

  //----------------------------------------------------------------------------
  // Iterator to check which patterns match the given key
  //----------------------------------------------------------------------------
  class Iterator {
  public:
    //--------------------------------------------------------------------------
    // Constructor
    //--------------------------------------------------------------------------
    Iterator(SimplePatternMatcher<T> *obj, const Key &k) :
      matcher(obj), key(k) {

      keyIterator = matcher->contents.getKeyIterator();
      advanceFirstIteratorUntilMatch();
    }

    //--------------------------------------------------------------------------
    // Check if iterator is valid
    //--------------------------------------------------------------------------
    bool valid() const {
      return isValid;
    }

    //--------------------------------------------------------------------------
    // Get pattern of item this iterator is pointing to
    //--------------------------------------------------------------------------
    Pattern getPattern() const {
      return keyIterator.getKey();
    }

    //--------------------------------------------------------------------------
    // Get value of item this iterator is pointing to
    //--------------------------------------------------------------------------
    T getValue() const {
      return matchIterator.getValue();
    }

    //--------------------------------------------------------------------------
    // Advance iterator
    //--------------------------------------------------------------------------
    void next() {
      matchIterator.next();

      if(!matchIterator.valid()) {
        keyIterator.next();
        advanceFirstIteratorUntilMatch();
      }
    }

    bool erase() {
      return matchIterator.erase();
    }

  private:
    //--------------------------------------------------------------------------
    // Advance first iterator until there's a pattern match
    //--------------------------------------------------------------------------
    void advanceFirstIteratorUntilMatch() {

      while(keyIterator.valid()) {
        Key iterKey = keyIterator.getKey();

        if(stringmatchlen(iterKey.data(), iterKey.size(), key.data(), key.size(), 0) == 1) {
          // We have a match
          matchIterator = matcher->contents.findMatching(iterKey);
          if(matchIterator.valid()) {
            return;
          }
        }

        keyIterator.next();
      }

      // No match, stop iterating
      isValid = false;
    }


    SimplePatternMatcher<T> *matcher;
    Key key;
    typename ThreadSafeMultiMap<Pattern, T>::KeyIterator keyIterator;
    typename ThreadSafeMultiMap<Pattern, T>::MatchIterator matchIterator;
    bool isValid = true;
  };

  //----------------------------------------------------------------------------
  // Find all patterns matching given value
  //----------------------------------------------------------------------------
  Iterator find(const Key& key) {
    return Iterator(this, key);
  }

  //----------------------------------------------------------------------------
  // Get iterator to full contents
  //----------------------------------------------------------------------------
  typename ThreadSafeMultiMap<Pattern, T>::FullIterator getFullIterator() {
    return contents.getFullIterator();
  }

private:
  ThreadSafeMultiMap<Pattern, T> contents;
};

}

#endif

