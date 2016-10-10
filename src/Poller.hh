// ----------------------------------------------------------------------
// File: Poller.hh
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

#ifndef __QUARKDB_POLLER_H__
#define __QUARKDB_POLLER_H__

#include <sstream>
#include <thread>
#include <atomic>
#include "Dispatcher.hh"
#include "EventFD.hh"

namespace quarkdb {

class Poller {
public:
  Poller(const std::string &path, Dispatcher *dispatcher);
  ~Poller();

private:
  std::atomic<bool> shutdown;
  std::atomic<int64_t> threadsAlive;

  EventFD shutdownFD;

  void main(Dispatcher *dispatcher);
  std::thread mainThread;

  std::string path;
  struct sockaddr_un local, remote;
  unsigned int s;
  size_t len;
  socklen_t t;
};




}

#endif
