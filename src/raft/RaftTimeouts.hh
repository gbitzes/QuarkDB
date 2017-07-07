// ----------------------------------------------------------------------
// File: RaftTimeouts.hh
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

#ifndef __QUARKDB_RAFT_TIMEOUT_H__
#define __QUARKDB_RAFT_TIMEOUT_H__

#include <chrono>
#include <random>
#include <mutex>
#include "../Utils.hh"

namespace quarkdb {
using std::chrono::milliseconds;

class RaftTimeouts {
public:
  RaftTimeouts(const milliseconds &low, const milliseconds &high,
    const milliseconds &heartbeat);

  milliseconds getLow() const;
  milliseconds getHigh() const;
  milliseconds getRandom() const;

  milliseconds getHeartbeatInterval() const;
private:
  milliseconds timeoutLow;
  milliseconds timeoutHigh;
  milliseconds heartbeatInterval;

  static std::random_device rd;
  static std::mt19937 gen;
  static std::mutex genMutex;
  mutable std::uniform_int_distribution<> dist;
};

class RaftClock {
public:
  RaftClock(const RaftTimeouts timeouts);
  DISALLOW_COPY_AND_ASSIGN(RaftClock);

  milliseconds refreshRandomTimeout();
  void heartbeat();
  bool timeout();

  RaftTimeouts getTimeouts() { return timeouts; }
  milliseconds getRandomTimeout();
  void triggerTimeout();
private:
  std::mutex lastHeartbeatMutex;
  std::chrono::steady_clock::time_point lastHeartbeat;
  milliseconds randomTimeout;

  RaftTimeouts timeouts;
  bool artificialTimeout = false;
};

extern RaftTimeouts defaultTimeouts;
extern RaftTimeouts tightTimeouts;
extern RaftTimeouts aggressiveTimeouts;

}

#endif
