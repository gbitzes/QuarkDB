// ----------------------------------------------------------------------
// File: Poller.cc
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

#include "Poller.hh"
#include "RedisParser.hh"
#include <sys/socket.h>
#include <sys/un.h>
#include <poll.h>

using namespace quarkdb;

Poller::Poller(const std::string &p, Dispatcher *dispatcher) : path(p) {
  unlink(p.c_str());
  if( (s = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
    perror("Poller, error opening socket");
  }
  local.sun_family = AF_UNIX;
  strcpy(local.sun_path, path.c_str());
  len = strlen(local.sun_path) + sizeof(local.sun_family);
  if(bind(s, (struct sockaddr *)&local, len) < 0) {
    perror("Poller, error when binding");
  }
  listen(s, 1);
  t = sizeof(remote);

  shutdown = false;
  mainThread = std::thread(&Poller::main, this, dispatcher);
}

Poller::~Poller() {
  shutdown = true;
  shutdownFD.notify();
  ::shutdown(s, SHUT_RDWR); // kill the socket
  mainThread.join();
  close(s);
  unlink(path.c_str());
}

void Poller::worker(int fd, Dispatcher *dispatcher) {
  XrdBuffManager bufferManager(NULL, NULL);
  Link link(fd);
  RedisParser parser(&link, &bufferManager);

  struct pollfd polls[2];
  polls[0].fd = fd;
  polls[0].events = POLLIN;
  polls[0].revents = 0;

  polls[1].fd = shutdownFD.getFD();
  polls[1].events = POLLIN;
  polls[1].revents = 0;

  RedisRequest currentRequest;

  while(!shutdown) {
    poll(polls, 2, -1);

    // time to quit?
    if(shutdown) break;

    while(true) {
      LinkStatus status = parser.fetch(currentRequest);
      if(status <= 0) break;
      dispatcher->dispatch(&link, currentRequest);
    }

    if( (polls[0].revents & POLLERR) || (polls[0].revents & POLLHUP) ) {
      break;
    }
  }
  close(fd);
}

void Poller::main(Dispatcher *dispatcher) {
  std::vector<std::thread> spawned;

  while(true) {
    int fd = accept(s, (struct sockaddr *)&remote, &t);
    if(fd < 0) break;

    spawned.emplace_back(&Poller::worker, this, fd, dispatcher);
  }

  for(std::thread &th : spawned) {
    th.join();
  }
}
