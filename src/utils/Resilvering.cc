// ----------------------------------------------------------------------
// File: Resilvering.cc
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

#include "../Utils.hh"
#include "Resilvering.hh"
#include "VectorUtils.hh"
#include "ParseUtils.hh"
using namespace quarkdb;

ResilveringEvent::ResilveringEvent()
: id("NULL"), startTime(0) { }

ResilveringEvent::ResilveringEvent(const ResilveringEventID &eventID, time_t start)
: id(eventID), startTime(start) {

}

std::string ResilveringEvent::serialize() const {
  return SSTR(id << " - " << startTime);
}

bool ResilveringEvent::deserialize(const std::string &str, ResilveringEvent &ret) {
  std::vector<std::string> parts = split(str, " - ");
  if(parts.size() != 2) return false;

  // Parse event ID
  ret.id = parts[0];

  // Parse time
  int64_t t;
  if(!ParseUtils::parseInt64(parts[1], t)) return false;
  ret.startTime = (time_t) t;

  return true;
}

ResilveringEventID ResilveringEvent::getID() const {
  return id;
}

time_t ResilveringEvent::getStartTime() const {
  return startTime;
}

bool ResilveringEvent::operator==(const ResilveringEvent& rhs) const {
  return (id == rhs.id)
      && (startTime == rhs.startTime);
}

ResilveringHistory::ResilveringHistory() {}

size_t ResilveringHistory::size() const {
  std::lock_guard<std::mutex> lock(mtx);
  return events.size();
}

std::string ResilveringHistory::serialize() const {
  std::lock_guard<std::mutex> lock(mtx);
  std::ostringstream ss;

  for(size_t i = 0; i < events.size(); i++) {
    ss << events[i].serialize() << "\n";
  }

  return ss.str();
}

void ResilveringHistory::append(const ResilveringEvent &event) {
  std::lock_guard<std::mutex> lock(mtx);
  events.push_back(event);
}

bool ResilveringHistory::deserialize(const std::string &str, ResilveringHistory &hist) {
  hist.events.clear();

  std::vector<std::string> parts = split(str, "\n");
  for(size_t i = 0; i < parts.size() - 1; i++) {
    ResilveringEvent ret;
    if(!ResilveringEvent::deserialize(parts[i], ret)) return false;

    hist.events.push_back(ret);
  }

  return true;
}

bool ResilveringHistory::operator==(const ResilveringHistory& rhs) const {
  std::lock(mtx, rhs.mtx);

  std::lock_guard<std::mutex> lock1(mtx, std::adopt_lock);
  std::lock_guard<std::mutex> lock2(rhs.mtx, std::adopt_lock);

  return VectorUtils::checkEquality(events, rhs.events);
}

const ResilveringEvent& ResilveringHistory::at(size_t i) const {
  std::lock_guard<std::mutex> lock(mtx);
  return events.at(i);
}

void ResilveringHistory::clear() {
  std::lock_guard<std::mutex> lock(mtx);
  events.clear();
}
