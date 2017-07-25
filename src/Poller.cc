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
#include <sys/ioctl.h>
#include <poll.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

using namespace quarkdb;

Poller::Poller(int port, Dispatcher *dispatcher) {
  struct addrinfo hints, *servinfo, *p;
  int rv, yes = 1;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE; // use my IP

  if ((rv = getaddrinfo(NULL, std::to_string(port).c_str(), &hints, &servinfo)) != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    exit(1);
  }

  // loop through all the results and bind to the first we can
  for(p = servinfo; p != NULL; p = p->ai_next) {
    if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      perror("socket");
      continue;
    }
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes,sizeof(int)) == -1) {
      perror("setsockopt");
      exit(1);
    }
    if (bind(s, p->ai_addr, p->ai_addrlen) == -1) {
      close(s);
      perror("bind");
      continue;
    }
    break;
  }

  freeaddrinfo(servinfo); // all done with this structure

  if (p == NULL) {
    fprintf(stderr, "server: failed to bind\n");
    exit(1);
  }

  if (listen(s, 10) == -1) {
    perror("listen");
    exit(1);
  }

  mainThread = std::thread(&Poller::main, this, dispatcher);
}

Poller::~Poller() {
  inFlightTracker.setAcceptingRequests(false);
  shutdownFD.notify();
  ::shutdown(s, SHUT_RDWR); // kill the socket
  mainThread.join();
  close(s);
}

void Poller::worker(int fd, Dispatcher *dispatcher) {
  qclient::TlsConfig tlsconfig;

  // peek first byte to determine if TLS is active
  char buffer[2];

  int ret = 0;
  while(ret == 0) {
    ret = recv(fd, buffer, 1, MSG_PEEK);
  }
  if(ret != 1) qdb_throw("unexpected result from recv: " << ret);

  tlsconfig.active = (buffer[0] != '*');
  Link link(fd, tlsconfig);
  Connection conn(&link);

  struct pollfd polls[2];
  polls[0].fd = fd;
  polls[0].events = POLLIN;
  polls[0].revents = 0;

  polls[1].fd = shutdownFD.getFD();
  polls[1].events = POLLIN;
  polls[1].revents = 0;

  while(inFlightTracker.isAcceptingRequests()) {
    poll(polls, 2, -1);

    // time to quit?
    if(!inFlightTracker.isAcceptingRequests()) break;

    // EOF?
    if(polls[0].revents & POLLIN) {
      int count = -1;
      ioctl(fd, FIONREAD, &count);
      if(count == 0) {
        break;
      }
    }

    LinkStatus status = conn.processRequests(dispatcher, inFlightTracker);
    if(status <= 0) break;

    if( (polls[0].revents & POLLERR) || (polls[0].revents & POLLHUP) ) {
      break;
    }
  }
}

void Poller::main(Dispatcher *dispatcher) {
  std::vector<std::thread> spawned;
  socklen_t remoteSize = sizeof(remote);

  while(true) {
    int fd = accept(s, (struct sockaddr *)&remote, &remoteSize);
    if(fd < 0) break;

    spawned.emplace_back(&Poller::worker, this, fd, dispatcher);
  }

  for(std::thread &th : spawned) {
    th.join();
  }
}
