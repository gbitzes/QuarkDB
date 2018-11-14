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

class Transaction;

class CommandMonitor {
public:
  CommandMonitor();

  void broadcast(std::string_view linkDescription, const RedisRequest& received);
  void broadcast(std::string_view linkDescription, const Transaction& transaction);

  void addRegistration(Connection *c);
  size_t size();

private:
  void broadcast(std::string_view linkDescription, std::string_view printableString);

  std::atomic<int64_t> active {false};
  std::mutex mtx;

  std::list<std::shared_ptr<PendingQueue>> monitors;
};

}

#endif
