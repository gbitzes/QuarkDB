// ----------------------------------------------------------------------
// File: HealthIndicator.hh
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

#ifndef QUARKDB_HEALTH_INDICATOR_HH
#define QUARKDB_HEALTH_INDICATOR_HH

#include "../utils/Macros.hh"
#include <string>
#include <string_view>
#include <vector>

namespace quarkdb {

enum class HealthStatus {
  kGreen,
  kYellow,
  kRed
};

//------------------------------------------------------------------------------
// Out of two health values, return the one that is the worst, ie:
// red, yellow => red
// red, green => red
// yellow, green => yellow
// green, green => green
//------------------------------------------------------------------------------
inline HealthStatus chooseWorstHealth(HealthStatus h1, HealthStatus h2) {
  if(h1 == HealthStatus::kGreen) {
    return h2;
  }

  if(h1 == HealthStatus::kYellow) {
    if(h2 == HealthStatus::kRed) return h2;
    return h1;
  }

  return h1;
}

inline std::string healthStatusAsString(HealthStatus st) {
  switch(st) {
    case HealthStatus::kGreen: {
      return "GREEN";
    }
    case HealthStatus::kYellow: {
      return "YELLOW";
    }
    case HealthStatus::kRed: {
      return "RED";
    }
    default: {
      qdb_throw("should never happen");
    }
  }
}

class HealthIndicator {
public:
  HealthIndicator(HealthStatus st, std::string_view desc, std::string_view msg)
  : status(st), description(desc), message(msg) {}

  HealthStatus getStatus() const {
    return status;
  }

  std::string getDescription() const {
    return description;
  }

  std::string getMessage() const {
    return message;
  }

  std::string toString() const {
    return SSTR("[" <<  healthStatusAsString(status) << "] " << description << ": " << message);
  }

private:
  const HealthStatus status;
  const std::string description;
  const std::string message;
};

inline std::vector<std::string> healthIndicatorsAsStrings(const std::vector<HealthIndicator> &source) {
  std::vector<std::string> out;

  for(size_t i = 0; i < source.size(); i++) {
    out.emplace_back(source[i].toString());
  }

  return out;
}

//------------------------------------------------------------------------------
// Out of many health indicators, return the worst health status.
//------------------------------------------------------------------------------
inline HealthStatus chooseWorstHealth(const std::vector<HealthIndicator> &vec) {
  HealthStatus val = HealthStatus::kGreen;
  for(size_t i = 0; i < vec.size(); i++) {
    val = chooseWorstHealth(val, vec[i].getStatus());
  }

  return val;
}

}

#endif
