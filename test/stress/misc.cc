// ----------------------------------------------------------------------
// File: misc.cc
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

#include "storage/ReverseLocator.hh"
#include "storage/KeyLocators.hh"
#include <gtest/gtest.h>
#include <random>

using namespace quarkdb;

static char numToChar(int n) {
  if(n == 0) {
    return 'a';
  }
  else if(n == 1) {
    return '#';
  }
  else if(n == 2) {
    return '|';
  }

  return 'x';
}

TEST(EscapedPrefixExtractor, BruteForce5CharCombinations) {
  for(int c1 = 0; c1 < 3; c1++) {
    for(int c2 = 0; c2 < 3; c2++) {
      for(int c3 = 0; c3 < 3; c3++) {
        for(int c4 = 0; c4 < 3; c4++) {
          for(int c5 = 0; c5 < 3; c5++) {
            for(int c6 = 0; c6 < 3; c6++) {
              std::string key;
              key.push_back(numToChar(c1));
              key.push_back(numToChar(c2));
              key.push_back(numToChar(c3));
              key.push_back(numToChar(c4));
              key.push_back(numToChar(c5));
              key.push_back(numToChar(c6));

              FieldLocator locator(KeyType::kHash, key, "field");
              std::string_view encoded = locator.toView();
              encoded.remove_prefix(1);

              EscapedPrefixExtractor extractor;
              ASSERT_TRUE(extractor.parse(encoded));

              ASSERT_EQ(extractor.getOriginalPrefix(), key);
              ASSERT_EQ(extractor.getRawSuffix(), "field");
            }
          }
        }
      }
    }
  }
}

TEST(EscapedPrefixExtractor, RandomizedTest) {
  std::mt19937 generator(8888);

  std::uniform_int_distribution<> lengthDistribution(0, 30);
  std::uniform_int_distribution<> charDistribution(0, 2);

  for(size_t round = 0; round < 5000000; round++) {
    size_t keyLength = lengthDistribution(generator);

    std::string key;
    for(size_t i = 0; i < keyLength; i++) {
      key.push_back(numToChar(charDistribution(generator)));
    }

    FieldLocator locator(KeyType::kHash, key, "field");
    std::string_view encoded = locator.toView();
    encoded.remove_prefix(1);

    EscapedPrefixExtractor extractor;
    ASSERT_TRUE(extractor.parse(encoded));

    ASSERT_EQ(extractor.getOriginalPrefix(), key);
    ASSERT_EQ(extractor.getRawSuffix(), "field");
  }
}
