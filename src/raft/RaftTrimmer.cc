// ----------------------------------------------------------------------
// File: RaftTrimmer.cc
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

#include "RaftTrimmer.hh"
#include "RaftJournal.hh"

using namespace quarkdb;


RaftTrimmer::RaftTrimmer(RaftJournal &jr)
: journal(jr), mainThread(&RaftTrimmer::main, this) {

}

void RaftTrimmer::main(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    LogIndex logSpan = journal.getLogSize() - journal.getLogStart();

    // TODO: make these configurable?
    if(logSpan >= 1000000) {
      journal.trimUntil(journal.getLogStart() + 100000);
    }
    else {
      assistant.wait_for(std::chrono::seconds(1));
    }
  }
}
