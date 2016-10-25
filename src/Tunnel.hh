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
#include <map>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include "EventFD.hh"

namespace quarkdb {

typedef std::shared_ptr<redisReply> redisReplyPtr;

class Tunnel {
public:
  Tunnel(const std::string &host, const int port, RedisRequest handshake = {});
  ~Tunnel();
  DISALLOW_COPY_AND_ASSIGN(Tunnel);

  // these two should have been private, but are called by an external callback
  void removeWriteNotification();
  void notifyWrite();

  void receivedDisconnect();
  void freeContext();

  std::future<redisReplyPtr> execute(const RedisRequest &req);
  std::future<redisReplyPtr> execute(size_t nchunks, const char **chunks, const size_t *sizes);

  //----------------------------------------------------------------------------
  // Convenience function, used mainly in tests.
  // This makes it possible to call exec("get", "key") instead of having to
  // build a vector.
  //
  // Extremely useful in macros, which don't support universal initialization.
  //----------------------------------------------------------------------------

  template<typename... Args>
  std::future<redisReplyPtr> exec(const Args... args) {
    return this->execute(RedisRequest {args...});
  }

  //----------------------------------------------------------------------------
  // Slight hack needed for unit tests. After an intercept has been added, any
  // connections to (host, ip) will be redirected to (host2, ip2) - usually
  // localhost.
  //----------------------------------------------------------------------------
  static void addIntercept(const std::string &host, const int port,
                           const std::string &host2, const int port2);
  static void clearIntercepts();
private:
  std::string host;
  int port;

  std::string targetHost;
  int targetPort;

  std::atomic<int64_t> shutdown {false};

  void startEventLoop();
  void eventLoop();
  void connect();
  void disconnect();
  void discoverIntercept();

  std::recursive_mutex asyncMutex;
  redisAsyncContext *asyncContext;
  EventFD writeEventFD;

  RedisRequest handshakeCommand;
  std::thread eventLoopThread;

  //----------------------------------------------------------------------------
  // We consult this map each time a new connection is to be opened
  //----------------------------------------------------------------------------
  static std::map<std::pair<std::string, int>, std::pair<std::string, int>> intercepts;
  static std::mutex interceptsMutex;
};


}

#endif
