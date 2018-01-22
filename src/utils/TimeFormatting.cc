// ----------------------------------------------------------------------
// File: TimeFormatting.cc
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

#include <sstream>
#include "Macros.hh"
#include "TimeFormatting.hh"

namespace quarkdb {

std::string formatTime(const std::chrono::seconds &totalSeconds) {
  std::chrono::seconds remainingSeconds = totalSeconds;
  std::stringstream ss;

  // Years
  auto years = std::chrono::duration_cast<Years>(remainingSeconds);
  remainingSeconds -= years;
  if(years.count() != 0) ss << years.count() << " years, ";

  // Months
  auto months = std::chrono::duration_cast<Months>(remainingSeconds);
  remainingSeconds -= months;
  if(months.count() != 0) ss << months.count() << " months, ";

  // Days
  auto days = std::chrono::duration_cast<Days>(remainingSeconds);
  remainingSeconds -= days;
  if(days.count() != 0) ss << days.count() << " days, ";

  // Hours
  auto hours = std::chrono::duration_cast<std::chrono::hours>(remainingSeconds);
  remainingSeconds -= hours;
  if(hours.count() != 0) ss << hours.count() << " hours, ";

  // Minutes
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(remainingSeconds);
  remainingSeconds -= minutes;
  if(minutes.count() != 0) ss << minutes.count() << " minutes, ";

  // Seconds
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(remainingSeconds);
  remainingSeconds -= seconds;
  ss << seconds.count() << " seconds";

  qdb_assert(remainingSeconds == std::chrono::seconds(0));
  return ss.str();
}

}
