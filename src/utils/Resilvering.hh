// ----------------------------------------------------------------------
// File: Resilvering.hh
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

#ifndef __QUARKDB_RESILVERING_H__
#define __QUARKDB_RESILVERING_H__

#include <string>

using ResilveringEventID = std::string;

class ResilveringEvent {
public:
  ResilveringEvent();
  ResilveringEvent(const ResilveringEventID &id, time_t finalized);

  ResilveringEventID getID();
  time_t getStartTime();

  std::string serialize();
  static bool deserialize(const std::string &str, ResilveringEvent &);

  bool operator==(const ResilveringEvent& rhs) const;
private:
  ResilveringEventID id;
  time_t startTime;
};

class ResilveringHistory {
public:
  ResilveringHistory();

  void append(const ResilveringEvent &event);
  size_t size();
  const ResilveringEvent& at(size_t i) const;

  std::string serialize();
  static bool deserialize(const std::string &str, ResilveringHistory &hist);

  bool operator==(const ResilveringHistory& rhs) const;
private:
  std::vector<ResilveringEvent> events;
};

#endif
