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
using namespace quarkdb;

LinkStatus PendingQueue::flushPending(const std::string &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  while(!pending.empty()) {
    if(conn) conn->writer.send(Formatter::err(msg));
    pending.pop();
  }
  if(conn) conn->writer.flush();
  return 1;
}

LinkStatus PendingQueue::appendResponse(std::string &&raw) {
  std::lock_guard<std::mutex> lock(mtx);
  if(!conn) qdb_throw("attempted to append a raw response to a pendingQueue while being detached from a Connection. Contents: '" << raw << "'");

  if(pending.empty()) return conn->writer.send(std::move(raw));

  // we're being blocked by a write, must queue
  PendingRequest req;
  req.rawResp = std::move(raw);
  pending.push(std::move(req));
  return 1;
}

LinkStatus PendingQueue::addPendingRequest(RedisDispatcher *dispatcher, RedisRequest &&req, LogIndex index) {
  std::lock_guard<std::mutex> lock(mtx);
  if(!conn) qdb_throw("attempted to append a pending request to a pendingQueue while being detached from a Connection, command " << req[0] << ", log index: " << index);

  if(pending.empty() && index < 0) return conn->writer.send(dispatcher->dispatch(req, 0));

  if(index > 0) {
    if(index <= lastIndex) {
      qdb_throw("attempted to insert queued request with index " << index
      << " while the last one had index " << lastIndex);
    }
    lastIndex = index;
  }

  PendingRequest penreq;
  penreq.req = std::move(req);
  penreq.index = index;
  pending.push(std::move(penreq));
  return 1;
}

LogIndex PendingQueue::dispatchPending(RedisDispatcher *dispatcher, LogIndex commitIndex) {
  std::lock_guard<std::mutex> lock(mtx);
  Connection::FlushGuard guard(conn);

  while(!pending.empty()) {
    PendingRequest &req = pending.front();
    if(commitIndex < req.index) {
      // the rest of the items are blocked, return new blocking index
      return req.index;
    }

    if(!req.rawResp.empty()) {
      if(conn) conn->writer.send(std::move(req.rawResp));
    }
    else {
      // we must dispatch the request even if the connection has died, since
      // writes increase lastApplied of the state machine
      std::string response = dispatcher->dispatch(req.req, commitIndex);
      if(conn) conn->writer.send(std::move(response));
    }

    pending.pop();
  }

  // no more pending requests
  return -1;
}

Connection::Connection(Link *l)
: writer(l), parser(l), pendingQueue(new PendingQueue(this)) {
}

Connection::~Connection() {
  pendingQueue->detachConnection();
}

LinkStatus Connection::raw(std::string &&raw) {
  return pendingQueue->appendResponse(std::move(raw));
}

LinkStatus Connection::err(const std::string &msg) {
  return pendingQueue->appendResponse(Formatter::err(msg));
}

LinkStatus Connection::errArgs(const std::string &cmd) {
  return pendingQueue->appendResponse(Formatter::errArgs(cmd));
}

LinkStatus Connection::pong() {
  return pendingQueue->appendResponse(Formatter::pong());
}

LinkStatus Connection::string(const std::string &str) {
  return pendingQueue->appendResponse(Formatter::string(str));
}

LinkStatus Connection::fromStatus(const rocksdb::Status &status) {
  return pendingQueue->appendResponse(Formatter::fromStatus(status));
}

LinkStatus Connection::status(const std::string &msg) {
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

LinkStatus Connection::scan(const std::string &marker, const std::vector<std::string> &vec) {
  return pendingQueue->appendResponse(Formatter::scan(marker, vec));
}

LinkStatus Connection::processRequests(Dispatcher *dispatcher, const std::atomic<bool> &stop) {
  FlushGuard guard(this);
  while(!stop) {
    LinkStatus status = parser.fetch(currentRequest);
    if(status == 0) return 1; // slow link
    if(status < 0) return status; // error
    dispatcher->dispatch(this, currentRequest);
  }
  return 1;
}

void Connection::setResponseBuffering(bool value) {
  writer.setActive(value);
}

void Connection::flush() {
  writer.flush();
}
