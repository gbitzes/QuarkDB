// ----------------------------------------------------------------------
// File: RaftTimeouts.cc
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

#include "RaftTimeouts.hh"
#include <random>

using namespace quarkdb;

RaftTimeouts quarkdb::defaultTimeouts(
  std::chrono::milliseconds(1000),
  std::chrono::milliseconds(1500),
  std::chrono::milliseconds(750));

RaftTimeouts quarkdb::tightTimeouts(
  std::chrono::milliseconds(100),
  std::chrono::milliseconds(150),
  std::chrono::milliseconds(75));

RaftTimeouts quarkdb::aggressiveTimeouts(
  std::chrono::milliseconds(5),
  std::chrono::milliseconds(10),
  std::chrono::milliseconds(1));

RaftTimeouts::RaftTimeouts(const std::chrono::milliseconds &low,
  const std::chrono::milliseconds &high,
  const std::chrono::milliseconds &heartbeat)
: timeoutLow(low), timeoutHigh(high), heartbeatInterval(heartbeat) {

}

std::chrono::milliseconds RaftTimeouts::getLow() const {
  return timeoutLow;
}

std::chrono::milliseconds RaftTimeouts::getHigh() const {
  return timeoutHigh;
}

std::chrono::milliseconds RaftTimeouts::getRandom() const {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(timeoutLow.count(), timeoutHigh.count());
  return std::chrono::milliseconds(dist(gen));
}

std::chrono::milliseconds RaftTimeouts::getHeartbeatInterval() const {
  return heartbeatInterval;
}
