// ----------------------------------------------------------------------
// File: Tunnel.hh
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

#ifndef __QUARKDB_TUNNEL_H__
#define __QUARKDB_TUNNEL_H__

#include "Utils.hh"
#include <mutex>
#include <future>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>

namespace quarkdb {

typedef std::shared_ptr<redisReply> redisReplyPtr;

class Tunnel {
public:
  Tunnel(const std::string &host, const int port);
  Tunnel(const std::string &unixSocket);
  ~Tunnel();
  DISALLOW_COPY_AND_ASSIGN(Tunnel);

  // these two should have been private, but are called by an external callback
  void removeWriteNotification();
  void notifyWrite();

  std::future<redisReplyPtr> execute(RedisRequest &req);
private:
  std::string host;
  int port;
  std::string unixSocket;
  std::atomic<int64_t> shutdown {0};

  void startEventLoop();
  void eventLoop();
  void connect();
  std::mutex asyncMutex;
  redisAsyncContext *asyncContext;
  int write_event_fd;

  RedisRequest handshakeCommand;
};


}

#endif
