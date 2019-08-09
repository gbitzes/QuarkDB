// ----------------------------------------------------------------------
// File: Publisher.cc
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

#include "pubsub/Publisher.hh"
#include "Formatter.hh"
#include "storage/PatternMatching.hh"

using namespace quarkdb;

// Constructor
Publisher::Publisher() {
  asyncPublishingThread.reset(&Publisher::asyncPublisher, this);
}

void Publisher::asyncPublisher(ThreadAssistant &assistant) {
  auto frontier = revisionQueue.begin();

  while(!assistant.terminationRequested()) {
    VersionedHashRevisionTracker *nextItem = frontier.getItemBlockOrNull();
    if(!nextItem) continue;

    for(auto it = nextItem->begin(); it != nextItem->end(); it++) {
      publish(it->first, it->second.serialize());
    }

    frontier.next();
    revisionQueue.pop_front();
  }
}

Publisher::~Publisher() {
  asyncPublishingThread.stop();
  revisionQueue.setBlockingMode(false);
  asyncPublishingThread.join();

  purgeListeners(Formatter::err("unavailable"));
}

void Publisher::purgeListeners(RedisEncodedResponse resp) {
  for(auto it = channelSubscriptions.getFullIterator(); it.valid(); it.next()) {
    it.getValue()->appendIfAttached(RedisEncodedResponse(resp));
    it.erase();
  }

  for(auto it = patternMatcher.getFullIterator(); it.valid(); it.next()) {
    it.getValue()->appendIfAttached(RedisEncodedResponse(resp));
    it.erase();
  }
}

bool Publisher::unsubscribe(std::shared_ptr<PendingQueue> connection, std::string_view channel) {
  connection->unsubscribe(std::string(channel));
  return channelSubscriptions.erase(std::string(channel), connection);
}

bool Publisher::punsubscribe(std::shared_ptr<PendingQueue> connection, std::string_view pattern) {
  connection->punsubscribe(std::string(pattern));
  return patternMatcher.erase(std::string(pattern), connection);
}

int Publisher::subscribe(std::shared_ptr<PendingQueue> connection, std::string_view channel) {
  connection->subscribe(std::string(channel));
  return channelSubscriptions.insert(std::string(channel), connection);
}

int Publisher::psubscribe(std::shared_ptr<PendingQueue> connection, std::string_view pattern) {
  connection->psubscribe(std::string(pattern));
  return patternMatcher.insert(std::string(pattern), connection);
}

int Publisher::publishChannels(const std::string &channel, std::string_view payload) {
  int hits = 0;

  // publish to matching channels
  for(auto it = channelSubscriptions.findMatching(std::string(channel)); it.valid(); it.next()) {
    bool stillAlive = it.getValue()->addMessageIfAttached(channel, Formatter::message(channel, payload));

    if(!stillAlive) {
      it.erase();
    }
    else {
      hits++;
    }
  }

  return hits;
}

int Publisher::publishPatterns(const std::string& channel, std::string_view payload) {
  int hits = 0;

  // publish to matching patterns
  for(auto it = patternMatcher.find(std::string(channel)); it.valid(); it.next()) {
    bool stillAlive = it.getValue()->addPatternMessageIfAttached(it.getPattern(), Formatter::pmessage(it.getPattern(), channel, payload));

    if(!stillAlive) {
      it.erase();
    }
    else {
      hits++;
    }
  }

  return hits;
}

int Publisher::publish(const std::string &channel, std::string_view payload) {
  return publishChannels(channel, payload) + publishPatterns(channel, payload);
}

LinkStatus Publisher::dispatch(Connection *conn, RedisRequest &req) {
  switch(req.getCommand()) {
    case RedisCommand::SUBSCRIBE: {
      if(req.size() <= 1) return conn->errArgs(req[0]);

      int retval = 1;
      for(size_t i = 1; i < req.size(); i++) {
        conn->getQueue()->subscriptions += subscribe(conn->getQueue(), req[i]);

        if(retval >= 0) {
          retval = conn->raw(Formatter::subscribe(req[i], conn->getQueue()->subscriptions));
        }
      }

      return retval;
    }
    case RedisCommand::PSUBSCRIBE: {
      if(req.size() <= 1) return conn->errArgs(req[0]);

      int retval = 1;
      for(size_t i = 1; i < req.size(); i++) {
        conn->getQueue()->subscriptions += psubscribe(conn->getQueue(), req[i]);

        if(retval >= 0) {
          retval = conn->raw(Formatter::psubscribe(req[i], conn->getQueue()->subscriptions));
        }
      }

      return retval;
    }
    case RedisCommand::UNSUBSCRIBE: {
      if(req.size() <= 1) return conn->errArgs(req[0]);

      int retval = 1;
      for(size_t i = 1; i < req.size(); i++) {
        conn->getQueue()->subscriptions -= unsubscribe(conn->getQueue(), req[i]);

        if(retval >= 0) {
          retval = conn->raw(Formatter::unsubscribe(req[i], conn->getQueue()->subscriptions));
        }
      }

      return retval;
    }
    case RedisCommand::PUNSUBSCRIBE: {
      if(req.size() <= 1) return conn->errArgs(req[0]);

      int retval = 1;
      for(size_t i = 1; i < req.size(); i++) {
        conn->getQueue()->subscriptions -= punsubscribe(conn->getQueue(), req[i]);

        if(retval >= 0) {
          retval = conn->raw(Formatter::punsubscribe(req[i], conn->getQueue()->subscriptions));
        }
      }

      return retval;
    }
    case RedisCommand::PUBLISH: {
      if(req.size() != 3) return conn->errArgs(req[0]);
      int hits = publish(std::string(req[1]), req[2]);
      return conn->integer(hits);
    }
    default: {
      qdb_throw("should never reach here");
    }
  }
}

void Publisher::schedulePublishing(VersionedHashRevisionTracker &&revisionTracker) {
  revisionQueue.emplace_back(std::move(revisionTracker));
}

LinkStatus Publisher::dispatch(Connection *conn, Transaction &tx) {
  qdb_throw("internal dispatching error, Publisher does not support transactions");
}


