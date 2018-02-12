// ----------------------------------------------------------------------
// File: RaftConfig.cc
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

#include "RaftConfig.hh"
#include "RaftDispatcher.hh"
#include "../Connection.hh"
#include "../StateMachine.hh"
#include "../utils/ParseUtils.hh"

using namespace quarkdb;

const std::string kTrimConfigKey("raft.trimming");
const std::string kResilveringEnabledKey("raft.resilvering.enabled");

bool TrimmingConfig::parse(const std::string &str) {
  std::vector<int64_t> parts;
  if(!ParseUtils::parseIntegerList(str, ":", parts)) {
    return false;
  }

  if(parts.size() != 2) {
    return false;
  }

  keepAtLeast = parts[0];
  step = parts[1];
  return true;
}

std::string TrimmingConfig::toString() const {
  return SSTR(keepAtLeast << ":" << step);
}

RaftConfig::RaftConfig(StateMachine &sm) : stateMachine(sm) {

}

bool RaftConfig::getResilveringEnabled() {
  std::string value;
  rocksdb::Status st = stateMachine.configGet(kResilveringEnabledKey, value);

  if(st.IsNotFound()) {
    return true;
  }

  if(!st.ok()) {
    qdb_throw("Error when retrieving whether resilvering is enabled: " << st.ToString());
  }

  if(value == "TRUE") {
    return true;
  }

  if(value == "FALSE") {
    return false;
  }

  qdb_throw("Invalid value for raft resilvering flag: " << value);
}

EncodedConfigChange RaftConfig::setResilveringEnabled(bool value) {
  RedisRequest req { "CONFIG_SET", kResilveringEnabledKey, boolToString(value) };
  return { "", req };
}

TrimmingConfig RaftConfig::getTrimmingConfig() {
  std::string trimConfig;
  rocksdb::Status st = stateMachine.configGet(kTrimConfigKey, trimConfig);

  if(st.IsNotFound()) {
    // Return default values
    return { 50000000, 1000000 };
  }
  else if(!st.ok()) {
    qdb_throw("Error when retrieving journal trim limit: " << st.ToString());
  }

  TrimmingConfig ret;
  if(!ret.parse(trimConfig)) {
    qdb_misconfig("Unable to parse trimming configuration key: " << kTrimConfigKey << " => " << trimConfig);
  }

  return ret;
}

EncodedConfigChange RaftConfig::setTrimmingConfig(const TrimmingConfig &trimConfig, bool overrideSafety) {
  // A 'keepAtLeast' value lower than 100k probably means an operator error.
  // By default, prevent such low values unless overrideSafety is set.
  if(!overrideSafety && trimConfig.keepAtLeast <= 100000) {
    qdb_critical("attempted to set journal 'keepAtLeast' configuration to very low value: " << trimConfig.keepAtLeast);
    return { SSTR("new 'keepAtLeast' too small: " << trimConfig.keepAtLeast), {} };
  }

  // A step value lower than 10k probably means an operator error.
  if(!overrideSafety && trimConfig.step <= 10000) {
    qdb_critical("attempted to set journal 'step' configuration to very low value: " << trimConfig.step);
    return { SSTR("new 'step' too small: " << trimConfig.step), {} };
  }

  RedisRequest req { "CONFIG_SET", kTrimConfigKey, trimConfig.toString() };
  return { "", req};
}
