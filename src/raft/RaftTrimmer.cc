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


RaftTrimmer::RaftTrimmer(RaftJournal &jr, RaftConfig &conf, StateMachine &sm)
: journal(jr), raftConfig(conf), stateMachine(sm), mainThread(&RaftTrimmer::main, this) {

}

void RaftTrimmer::main(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    LogIndex start, size, threshold;
    TrimmingConfig trimConfig;

    // Don't trim at all if resilvering is going on.
    if(resilveringsInProgress != 0) goto wait;

    start = journal.getLogStart();
    size = journal.getLogSize();

    trimConfig = raftConfig.getTrimmingConfig();

    // If we removed 'step' entries, would we still have at least 'keepAtLeast'
    // entries in the journal?
    if(size - start <= trimConfig.keepAtLeast + trimConfig.step) {
      goto wait;
    }

    threshold = start + trimConfig.step;

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

void RaftTrimmer::resilveringInitiated() {
  std::lock_guard<std::mutex> lock(mtx);
  resilveringsInProgress++;

  if(resilveringsInProgress == 1) {
    qdb_info("Pausing journal trimming, as this node is about to start resilvering another.");
  }
}

void RaftTrimmer::resilveringOver() {
  std::lock_guard<std::mutex> lock(mtx);
  resilveringsInProgress--;

  qdb_assert(resilveringsInProgress >= 0);

  if(resilveringsInProgress == 0) {
    qdb_info("No resilvering is in progress, resuming journal trimmer.");
  }
}
