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

using namespace quarkdb;

// TODO: implement internal commands in stateMachine, so such keys are hidden from
// users
const std::string trimLimitConfigKey("__internal_raft_config_trim_limit");

RaftConfig::RaftConfig(RaftDispatcher &disp, StateMachine &sm)
: dispatcher(disp), stateMachine(sm) {

}

int64_t RaftConfig::getJournalTrimLimit() {
  std::string trimLimit;
  rocksdb::Status st = stateMachine.get(trimLimitConfigKey, trimLimit);

  if(st.IsNotFound()) {
    // Return default value
    return 1000000;
  }
  else if(!st.ok()) {
    qdb_throw("Error when retrieving journal trim limit: " << st.ToString());
  }

  return binaryStringToInt(trimLimit.c_str());
}

// A value lower than 100k probably means an operator error.
// By default, prevent such low values unless overrideSafety is set.
LinkStatus RaftConfig::setJournalTrimLimit(Connection *conn, int64_t newLimit, bool overrideSafety) {
  if(!overrideSafety && newLimit <= 100000) {
    qdb_critical("attempted to set journal trimming limit to very low value: " << newLimit);
    return conn->err(SSTR("new limit too small: " << newLimit));
  }

  if(newLimit < 3) {
    qdb_critical("attempted to set journal trimming limit to very low value: " << newLimit);
    return conn->err(SSTR("new limit too small, even with overrideSafety: " << newLimit));
  }

  RedisRequest req { "SET", trimLimitConfigKey, intToBinaryString(newLimit) };
  return dispatcher.dispatch(conn, req);
}
