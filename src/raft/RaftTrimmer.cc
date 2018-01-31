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
#include "RaftConfig.hh"
#include "../StateMachine.hh"

using namespace quarkdb;

RaftTrimmingBlock::RaftTrimmingBlock(RaftTrimmer &tr, LogIndex preserve)
: trimmer(tr) {
  enforce(preserve);
}

RaftTrimmingBlock::~RaftTrimmingBlock() {
  lift();
}

LogIndex RaftTrimmingBlock::getPreservationIndex() const {
  return preserveIndex;
}

void RaftTrimmingBlock::enforce(LogIndex limit) {
  preserveIndex = limit;

  if(registered && limit == std::numeric_limits<LogIndex>::max()) {
    trimmer.registerChange(this);
    registered = false;
  }
  else if(!registered && limit != std::numeric_limits<LogIndex>::max()) {
    trimmer.registerChange(this);
    registered = true;
  }
}

void RaftTrimmingBlock::lift() {
  enforce(std::numeric_limits<LogIndex>::max());
}

RaftTrimmer::RaftTrimmer(RaftJournal &jr, RaftConfig &conf, StateMachine &sm)
: journal(jr), raftConfig(conf), stateMachine(sm), mainThread(&RaftTrimmer::main, this) {

}

void RaftTrimmer::registerChange(RaftTrimmingBlock* block) {
  std::lock_guard<std::mutex> guard(mtx);

  // De-register?
  if(block->getPreservationIndex() == std::numeric_limits<LogIndex>::max()) {
    blocks.erase(block);
    return;
  }

  // Nope, enforce
  blocks.insert(block);
}

bool RaftTrimmer::canTrimUntil(LogIndex threshold) {
  std::lock_guard<std::mutex> lock(mtx);

  for(auto it = blocks.begin(); it != blocks.end(); it++) {
    if((*it)->getPreservationIndex() <= threshold) {
      return false;
    }
  }

  return true;
}

void RaftTrimmer::main(ThreadAssistant &assistant) {
  // std::chrono::steady_clock::time_point lastMessage;

  while(!assistant.terminationRequested()) {
    LogIndex start, size, threshold;
    TrimmingConfig trimConfig;

    start = journal.getLogStart();
    size = journal.getLogSize();

    trimConfig = raftConfig.getTrimmingConfig();

    // If we removed 'step' entries, would we still have at least 'keepAtLeast'
    // entries in the journal?
    if(size - start <= trimConfig.keepAtLeast + trimConfig.step) {
      goto wait;
    }

    threshold = start + trimConfig.step;

    // Check if any trimming block is preserving these entries.
    if(!canTrimUntil(threshold)) {
      goto wait;
    }

    // A last, paranoid check: Have the entries we're about to remove been
    // both committed and applied?
    if(journal.getCommitIndex() <= threshold || stateMachine.getLastApplied() <= threshold) {
      goto wait;
    }

    // All clear, go.
    journal.trimUntil(threshold);
    continue; // no wait

wait:
      assistant.wait_for(std::chrono::seconds(1));
  }
}
