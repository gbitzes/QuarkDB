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

namespace quarkdb {

class RaftTimeouts {
public:
  RaftTimeouts(const std::chrono::milliseconds &low,
    const std::chrono::milliseconds &high,
    const std::chrono::milliseconds &heartbeat);

  std::chrono::milliseconds getLow() const;
  std::chrono::milliseconds getHigh() const;
  std::chrono::milliseconds getRandom() const;

  std::chrono::milliseconds getHeartbeatInterval() const;
private:
  std::chrono::milliseconds timeoutLow;
  std::chrono::milliseconds timeoutHigh;
  std::chrono::milliseconds heartbeatInterval;
};

extern RaftTimeouts relaxedTimeouts;
extern RaftTimeouts defaultTimeouts;
extern RaftTimeouts tightTimeouts;
extern RaftTimeouts aggressiveTimeouts;

}

#endif
