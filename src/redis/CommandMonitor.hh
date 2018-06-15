// ----------------------------------------------------------------------
// File: CommandMonitor.hh
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

#ifndef QUARKDB_COMMAND_MONITOR_H
#define QUARKDB_COMMAND_MONITOR_H

#include <list>
#include "../Connection.hh"

namespace quarkdb {

class MultiOp;

class CommandMonitor {
public:
  CommandMonitor();

  void broadcast(const std::string& linkDescription, const RedisRequest& received);
  void broadcast(const std::string& linkDescription, const MultiOp& multiOp);

  void addRegistration(Connection *c);
  size_t size();

private:
  void broadcast(const std::string& linkDescription, const std::string& printableString);
  
  std::atomic<int64_t> active {false};
  std::mutex mtx;

  std::list<std::shared_ptr<PendingQueue>> monitors;
};

}

#endif
