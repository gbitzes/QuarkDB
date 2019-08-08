// ----------------------------------------------------------------------
// File: ExpirationEventIterator.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "storage/ExpirationEventIterator.hh"
#include "storage/StagingArea.hh"
#include "storage/KeyLocators.hh"
#include "utils/IntToBinaryString.hh"

using namespace quarkdb;

ExpirationEventIterator::ExpirationEventIterator(StagingArea &st)
: stagingArea(st), iter(stagingArea.getIterator()) {

  std::string searchPrefix(1, char(InternalKeyType::kExpirationEvent));
  iter->Seek(searchPrefix);
  assertDeadlineSanity();
}

ExpirationEventIterator::~ExpirationEventIterator() {

}

bool ExpirationEventIterator::valid() {
  if(!iter) return false;
  if(!iter->Valid()) {
    iter.reset();
    return false;
  }

  if(iter->key()[0] != char(InternalKeyType::kExpirationEvent)) {
    iter.reset();
    return false;
  }

  return true;
}

void ExpirationEventIterator::next() {
  iter->Next();
  assertDeadlineSanity();
}

void ExpirationEventIterator::assertDeadlineSanity() {
  if(valid()) {
    ClockValue deadline = getDeadline();
    qdb_assert(lastDeadline <= deadline);
    lastDeadline = deadline;
  }
}

ClockValue ExpirationEventIterator::getDeadline() {
  return binaryStringToUnsignedInt(iter->key().data()+1);
}

std::string_view ExpirationEventIterator::getRedisKey() {
  constexpr size_t offset = 1 + sizeof(ClockValue);
  return std::string_view(iter->key().data()+offset, iter->key().size()-offset);
}
