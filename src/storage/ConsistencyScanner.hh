// ----------------------------------------------------------------------
// File: ChecksumScanner.hh
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

#ifndef __QUARKDB_CONSISTENCY_SCANNER_H__
#define __QUARKDB_CONSISTENCY_SCANNER_H__

#include "../utils/AssistedThread.hh"

namespace quarkdb {

class StateMachine;

class ConsistencyScanner {
public:
  ConsistencyScanner(StateMachine &stateMachine);
  void main(ThreadAssistant &assistant);
  void nextPass(ThreadAssistant &assistant);
  void singlePass();

  static std::chrono::seconds obtainScanPeriod(StateMachine &stateMachine);
  static const std::chrono::seconds kDefaultPeriod;
  static const std::string kConfigurationKey;
private:
  std::mutex mtx;
  StateMachine &stateMachine;
  AssistedThread thread;
};

}

#endif
