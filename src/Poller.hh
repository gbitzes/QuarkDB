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
#include "Utils.hh"
#include "Dispatcher.hh"
#include "EventFD.hh"
#include <netinet/in.h>

namespace quarkdb {

class Poller {
public:
  Poller(int port, Dispatcher *dispatcher);
  ~Poller();
  DISALLOW_COPY_AND_ASSIGN(Poller);

private:
  std::atomic<bool> shutdown;

  EventFD shutdownFD;

  void main(Dispatcher *dispatcher);
  void worker(int fd, Dispatcher *dispatcher);

  std::thread mainThread;

  struct sockaddr_in local, remote;
  int s;
  size_t len;
};

}

#endif
