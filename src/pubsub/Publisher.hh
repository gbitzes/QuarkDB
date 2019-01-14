// ----------------------------------------------------------------------
// File: Publisher.hh
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

#ifndef QUARKDB_PUBLISHER_HH
#define QUARKDB_PUBLISHER_HH

#include "SimplePatternMatcher.hh"
#include "../Connection.hh"
#include "../Dispatcher.hh"
#include <map>
#include <mutex>
#include <memory>
#include <set>

namespace quarkdb {

class PendingQueue;
class RedisRequest;

class Publisher : public Dispatcher {
public:
  // Destructor
  ~Publisher();

  // Subscribe connection to given channel or pattern. Return whether the subscription
  // existed already, or not.
  int subscribe(std::shared_ptr<PendingQueue> connection, std::string_view channel);
  int psubscribe(std::shared_ptr<PendingQueue> connection, std::string_view pattern);

  int publish(std::string_view channel, std::string_view payload);
  void purgeListeners(RedisEncodedResponse resp);

  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;
  virtual LinkStatus dispatch(Connection *conn, Transaction &tx) override final;
  virtual void notifyDisconnect(Connection *conn) override final {}

private:
  int publishChannels(std::string_view channel, std::string_view payload);
  int publishPatterns(std::string_view channel, std::string_view payload);
  bool unsubscribe(std::shared_ptr<PendingQueue> connection, std::string_view channel);
  bool punsubscribe(std::shared_ptr<PendingQueue> connection, std::string_view pattern);

  std::mutex mtx;

  // Map of subscribed-to channels
  ThreadSafeMultiMap<std::string, std::shared_ptr<PendingQueue>> channelSubscriptions;

  // Pattern matcher
  SimplePatternMatcher<std::shared_ptr<PendingQueue>> patternMatcher;
};

}

#endif
