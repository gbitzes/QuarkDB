// ----------------------------------------------------------------------
// File: RaftConfig.hh
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

#ifndef __QUARKDB_RAFT_CONFIG_H__
#define __QUARKDB_RAFT_CONFIG_H__

#include <stdint.h>
#include "../Link.hh"

namespace quarkdb {

class StateMachine; class RaftDispatcher; class Connection;

struct TrimmingConfig {
  // Minimum number of journal entries to keep at all times.
  int64_t keepAtLeast;
  // Trimming step - don't bother to trim if we'd be getting rid of fewer than
  // step entries.
  int64_t step;
};

class RaftConfig {
public:
  RaftConfig(RaftDispatcher &dispatcher, StateMachine &sm);

  TrimmingConfig getTrimmingConfig();
  LinkStatus setTrimmingConfig(Connection *conn, const TrimmingConfig &trimConfig, bool overrideSafety = false);

private:
  RaftDispatcher &dispatcher;
  StateMachine &stateMachine;
};

}


#endif
