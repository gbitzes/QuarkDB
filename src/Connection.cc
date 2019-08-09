//----------------------------------------------------------------------
// File: Connection.cc
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

#include "Connection.hh"
#include "Dispatcher.hh"
#include "Formatter.hh"
#include "utils/InFlightTracker.hh"
#include "redis/InternalFilter.hh"
using namespace quarkdb;

LinkStatus PendingQueue::flushPending(const RedisEncodedResponse &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  while(!pending.empty()) {
    if(conn) {
      if(!pending.front().rawResp.empty()) {
        conn->writer.send(std::move(pending.front().rawResp.val));
      }
      else {
        conn->writer.send(Formatter::multiply(msg, pending.front().tx.expectedResponses() ).val);
      }
    }
    pending.pop();
  }
  if(conn) conn->writer.flush();
  lastIndex = -1;
  return 1;
}

void PendingQueue::subscribe(const std::string &item) {
  std::lock_guard<std::mutex> lock(mtx);
  subscriptionTracker.addChannel(item);
}

void PendingQueue::psubscribe(const std::string &item) {
  std::lock_guard<std::mutex> lock(mtx);
  subscriptionTracker.addPattern(item);
}

void PendingQueue::unsubscribe(const std::string &item) {
  std::lock_guard<std::mutex> lock(mtx);
  subscriptionTracker.removeChannel(item);
}

void PendingQueue::punsubscribe(const std::string &item) {
  std::lock_guard<std::mutex> lock(mtx);
  subscriptionTracker.removePattern(item);
}

bool PendingQueue::addMessageIfAttached(const std::string &channel, RedisEncodedResponse &&raw) {
  std::lock_guard<std::mutex> lock(mtx);
  if(!conn) return false;

  if(!subscriptionTracker.hasChannel(channel)) return true;
  Connection::FlushGuard guard(conn);
  appendResponseNoLock(std::move(raw));
  return true;
}

bool PendingQueue::addPatternMessageIfAttached(const std::string &pattern, RedisEncodedResponse &&raw) {
  std::lock_guard<std::mutex> lock(mtx);
  if(!conn) return false;

  if(!subscriptionTracker.hasPattern(pattern)) return true;
  Connection::FlushGuard guard(conn);
  appendResponseNoLock(std::move(raw));
  return true;
}

bool PendingQueue::appendIfAttached(RedisEncodedResponse &&raw) {
  std::lock_guard<std::mutex> lock(mtx);

  if(!conn) return false;
  Connection::FlushGuard guard(conn);
  appendResponseNoLock(std::move(raw));
  return true;
}

LinkStatus PendingQueue::appendResponseNoLock(RedisEncodedResponse &&raw) {
  if(!conn) qdb_throw("attempted to append a raw response to a pendingQueue while being detached from a Connection. Contents: '" << raw.val << "'");

  if(pending.empty()) return conn->writer.send(std::move(raw.val));

  // we're being blocked by a write, must queue
  PendingRequest req;
  req.rawResp = std::move(raw);
  pending.push(std::move(req));
  return 1;
}

LinkStatus PendingQueue::appendResponse(RedisEncodedResponse &&raw) {
  std::lock_guard<std::mutex> lock(mtx);
  return appendResponseNoLock(std::move(raw));
}

LinkStatus PendingQueue::addPendingTransaction(RedisDispatcher *dispatcher, Transaction &&tx, LogIndex index) {
  std::lock_guard<std::mutex> lock(mtx);
  if(!conn) qdb_throw("attempted to append a pending request to a pendingQueue while being detached from a Connection, command " << tx.toPrintableString() << ", log index: " << index);

  if(pending.empty() && index < 0) {
    // This is a read, and we're not being blocked by any writes. Forward directly
    // to the state machine, no need to do any queueing.
    qdb_assert(!tx.containsWrites());
    return conn->writer.send(dispatcher->dispatch(tx, 0).val);
  }

  if(index > 0) {
    if(index <= lastIndex) {
      qdb_throw("attempted to insert queued request with index " << index
      << " while the last one had index " << lastIndex);
    }
    lastIndex = index;
  }

  PendingRequest penreq;
  penreq.tx = std::move(tx);
  penreq.index = index;
  pending.push(std::move(penreq));
  return 1;
}

LogIndex PendingQueue::dispatchPending(RedisDispatcher *dispatcher, LogIndex commitIndex) {
  std::lock_guard<std::mutex> lock(mtx);
  Connection::FlushGuard guard(conn);
  bool found = false;

  while(!pending.empty()) {
    PendingRequest &req = pending.front();
    if(commitIndex < req.index) {
      // the rest of the items are blocked, return new blocking index
      return req.index;
    }

    if(!req.rawResp.empty()) {
      if(conn) conn->writer.send(std::move(req.rawResp.val));
    }
    else {
      if(req.index > 0) {
        if(found) qdb_throw("queue corruption: " << this << " found entry with positive index twice (" << req.index << ")");
        found = true;
        if(req.index != commitIndex) qdb_throw("queue corruption: " << this << " expected entry with index " << commitIndex << ", found " << req.index);
      }

      // we must dispatch the request even if the connection has died, since
      // writes increase lastApplied of the state machine
      RedisEncodedResponse response = dispatcher->dispatch(req.tx, req.index);
      if(conn) conn->writer.send(std::move(response.val));
    }

    pending.pop();
  }

  if(!found) qdb_throw("entry with index " << commitIndex << " not found");

  // no more pending requests
  return -1;
}

void PendingQueue::activatePushTypes() {
  supportsPushTypes = true;
}

size_t phantomBatchLimit = 100;

void Connection::setPhantomBatchLimit(size_t newval) {
  phantomBatchLimit = newval;
}

Connection::Connection(Link *l)
: writer(l), parser(l), pendingQueue(new PendingQueue(this)),
  description(l->describe()), uuid(l->getID()), localhost(l->isLocalhost()) {
}

Connection::~Connection() {
  pendingQueue->detachConnection();
}

LinkStatus Connection::raw(RedisEncodedResponse &&encoded) {
  return pendingQueue->appendResponse(std::move(encoded));
}

LinkStatus Connection::moved(int64_t shardId, const RaftServer &location) {
  return pendingQueue->appendResponse(Formatter::moved(shardId, location));
}

LinkStatus Connection::err(std::string_view msg) {
  return pendingQueue->appendResponse(Formatter::err(msg));
}

LinkStatus Connection::errArgs(std::string_view cmd) {
  return pendingQueue->appendResponse(Formatter::errArgs(cmd));
}

LinkStatus Connection::pong() {
  return pendingQueue->appendResponse(Formatter::pong());
}

LinkStatus Connection::string(std::string_view str) {
  return pendingQueue->appendResponse(Formatter::string(str));
}

LinkStatus Connection::fromStatus(const rocksdb::Status &status) {
  return pendingQueue->appendResponse(Formatter::fromStatus(status));
}

LinkStatus Connection::status(std::string_view msg) {
  return pendingQueue->appendResponse(Formatter::status(msg));
}

LinkStatus Connection::ok() {
  return pendingQueue->appendResponse(Formatter::ok());
}

LinkStatus Connection::null() {
  return pendingQueue->appendResponse(Formatter::null());
}

LinkStatus Connection::integer(int64_t number) {
  return pendingQueue->appendResponse(Formatter::integer(number));
}

LinkStatus Connection::vector(const std::vector<std::string> &vec) {
  return pendingQueue->appendResponse(Formatter::vector(vec));
}

LinkStatus Connection::statusVector(const std::vector<std::string> &vec) {
  return pendingQueue->appendResponse(Formatter::statusVector(vec));
}

LinkStatus Connection::scan(std::string_view marker, const std::vector<std::string> &vec) {
  return pendingQueue->appendResponse(Formatter::scan(marker, vec));
}

LinkStatus Connection::noauth(std::string_view msg) {
  return pendingQueue->appendResponse(Formatter::noauth(msg));
}

LinkStatus Connection::processRequests(Dispatcher *dispatcher, const InFlightTracker &inFlightTracker) {
  FlushGuard guard(this);

  while(inFlightTracker.isAcceptingRequests()) {
    if(monitor) {
      // This connection is in "MONITOR" mode, we don't accept any more commands.
      // Do nothing for all received data.

      LinkStatus status = parser.purge();
      if(status == 0) return 1; // slow link
      if(status < 0) return status; // link error
      qdb_throw("should never reach here");
    }

    LinkStatus status = parser.fetch(currentRequest);
    InternalFilter::process(currentRequest);

    if(status < 0) {
      return status; // link error
    }

    if(status == 0) {
      // slow link - process the write batch, if needed
      multiHandler.finalizePhantomTransaction(dispatcher, this);
      return 1; // slow link
    }

    // Beginning of a MULTI block: Finalize phantom transactions
    if(currentRequest.getCommand() == RedisCommand::MULTI) {
      multiHandler.finalizePhantomTransaction(dispatcher, this);
      multiHandler.process(dispatcher, this, currentRequest);
      continue;
    }

    // EXEC without MULTI?
    if(currentRequest.getCommand() == RedisCommand::EXEC && !multiHandler.active()) {
      this->err("EXEC without MULTI");
      continue;
    }

    if(currentRequest.getCommand() == RedisCommand::TX_READWRITE) {
      multiHandler.finalizePhantomTransaction(dispatcher, this);
      dispatcher->dispatch(this, currentRequest);
      continue;
    }

    if(multiHandler.size() >= phantomBatchLimit) {
      multiHandler.finalizePhantomTransaction(dispatcher, this);
    }

    if(multiHandler.active()) {
      if(multiHandler.isPhantom() && currentRequest.getCommandType() != CommandType::WRITE) {
        multiHandler.finalizePhantomTransaction(dispatcher, this);
      }
      else {
        multiHandler.process(dispatcher, this, currentRequest);
        continue;
      }
    }

    if(currentRequest.getCommand() == RedisCommand::DISCARD) {
      multiHandler.finalizePhantomTransaction(dispatcher, this);
      this->err("DISCARD without MULTI");
      continue;
    }

    if(currentRequest.getCommandType() == CommandType::WRITE) {
      multiHandler.activatePhantom();
      multiHandler.process(dispatcher, this, currentRequest);
      continue;
    }
    else {
      multiHandler.finalizePhantomTransaction(dispatcher, this);
      dispatcher->dispatch(this, currentRequest);
    }
  }

  multiHandler.finalizePhantomTransaction(dispatcher, this);
  return 1;
}

void Connection::setResponseBuffering(bool value) {
  writer.setActive(value);
}

void Connection::flush() {
  writer.flush();
}

std::string Connection::describe() const {
  return description;
}

void Connection::activatePushTypes() {
  pendingQueue->activatePushTypes();
}
