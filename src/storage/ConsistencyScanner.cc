// ----------------------------------------------------------------------
// File: ConsistencyScanner.cc
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

#include "storage/ConsistencyScanner.hh"
#include "StateMachine.hh"
#include "utils/ParseUtils.hh"

using namespace quarkdb;

const std::chrono::seconds ConsistencyScanner::kDefaultPeriod = std::chrono::hours(12);
const std::string ConsistencyScanner::kConfigurationKey = "state-machine.consistency-check.period";

ConsistencyScanner::ConsistencyScanner(StateMachine &sm) : stateMachine(sm) {
  thread.reset(&ConsistencyScanner::main, this);
  thread.setName("checksum-scanner");
}

void ConsistencyScanner::singlePass() {
  std::lock_guard<std::mutex> lock(mtx);

  rocksdb::Status status = stateMachine.verifyChecksum();
  if(!status.ok()) {
    qdb_throw("State machine corruption, checksum calculation failed: " << status.ToString());
  }
}

std::chrono::seconds ConsistencyScanner::obtainScanPeriod(StateMachine &stateMachine) {
  std::string value;
  rocksdb::Status st = stateMachine.configGet(kConfigurationKey, value);

  if(st.IsNotFound()) {
    return kDefaultPeriod;
  }

  if(!st.ok()) {
    qdb_throw("Unexpected rocksdb status when retrieving " << kConfigurationKey << ": " << st.ToString());
  }

  int64_t period;
  if(!ParseUtils::parseInt64(value, period) || period < 0) {
    qdb_critical("Unable to parse " << kConfigurationKey << ": " << value << ", possible misconfiguration.");
    return kDefaultPeriod;
  }

  return std::chrono::seconds(period);
}

void ConsistencyScanner::nextPass(ThreadAssistant &assistant) {
  std::chrono::steady_clock::time_point startTime = std::chrono::steady_clock::now();

  while(!assistant.terminationRequested()) {
    std::chrono::steady_clock::time_point deadline = startTime + obtainScanPeriod(stateMachine);

    if(deadline <= std::chrono::steady_clock::now()) {
      singlePass();
      return;
    }

    // TODO: Not so nice, find a way to notify this thread whenever the period changes
    assistant.wait_for(std::chrono::seconds(1));
  }
}

void ConsistencyScanner::main(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    nextPass(assistant);
  }
}
