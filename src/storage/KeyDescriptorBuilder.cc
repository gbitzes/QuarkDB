// ----------------------------------------------------------------------
// File: KeyDescriptorBuilder.cc
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

#include "KeyDescriptorBuilder.hh"
#include "ReverseLocator.hh"
#include "../StateMachine.hh"


using namespace quarkdb;

static void appendToWriteBatch(std::string &prefix, std::string &key, KeyDescriptor &descriptor, rocksdb::WriteBatch &wb) {
  if(!key.empty()) {
    DescriptorLocator dlocator(key);
    wb.Put(dlocator.toSlice(), descriptor.serialize());
  }

  prefix.clear();
  key.clear();
  descriptor = KeyDescriptor();
}

KeyDescriptorBuilder::KeyDescriptorBuilder(StateMachine &stateMachine) {
  StateMachine::IteratorPtr iterator = stateMachine.getRawIterator();

  qdb_event("Scanning entire database to calculate key descriptors...");

  rocksdb::WriteBatch descriptorBatch;

  std::string currentPrefix;
  std::string currentKey;
  KeyDescriptor descriptor;

  bool firstIter = true;
  while(true) {
    if(firstIter) {
      iterator->SeekToFirst();
      firstIter = false;
    }
    else {
      iterator->Next();
    }

    if(!iterator->Valid()) {
      appendToWriteBatch(currentPrefix, currentKey, descriptor, descriptorBatch);
      break;
    }

    if(iterator->key()[0] == char(InternalKeyType::kInternal)) {
      // skip
      continue;
    }

    ReverseLocator revlocator(iterator->key());

    if(revlocator.getKeyType() == KeyType::kParseError) {
      qdb_critical("Unable to parse key when rebuilding key descriptors: " << iterator->key().ToString());
      continue;
    }

    if(revlocator.getKeyType() == KeyType::kString) {
      appendToWriteBatch(currentPrefix, currentKey, descriptor, descriptorBatch);

      currentKey = revlocator.getOriginalKey().ToString();
      descriptor.setKeyType(KeyType::kString);
      descriptor.setSize(iterator->value().size());

      appendToWriteBatch(currentPrefix, currentKey, descriptor, descriptorBatch);
      continue;
    }

    // We're dealing with a key that has prefix ..
    if(currentPrefix != revlocator.getRawPrefix()) {
      appendToWriteBatch(currentPrefix, currentKey, descriptor, descriptorBatch);

      currentPrefix = revlocator.getRawPrefix().ToString();
      currentKey = revlocator.getOriginalKey().ToString();

      descriptor.setKeyType(revlocator.getKeyType());
      descriptor.setSize(0);
    }

    if(!revlocator.isLocalityIndex()) {
      descriptor.setSize(descriptor.getSize() + 1);
    }
  }

  qdb_event("Collected " << descriptorBatch.Count() << " descriptors. Flushing write batch..");
  stateMachine.commitBatch(descriptorBatch);
}
