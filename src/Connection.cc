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

Connection::Connection(Link *l)
: link(l) {
}

Connection::~Connection() {
  flushPending("ERR connection shutting down");
}

LinkStatus Connection::raw(std::string &&raw) {
  return this->append(std::move(raw));
}

LinkStatus Connection::err(const std::string &msg) {
  return this->append(Formatter::err(msg));
}

LinkStatus Connection::errArgs(const std::string &cmd) {
  return this->append(Formatter::errArgs(cmd));
}

LinkStatus Connection::pong() {
  return this->append(Formatter::pong());
}

LinkStatus Connection::string(const std::string &str) {
  return this->append(Formatter::string(str));
}

LinkStatus Connection::fromStatus(const rocksdb::Status &status) {
  return this->append(Formatter::fromStatus(status));
}

LinkStatus Connection::status(const std::string &msg) {
  return this->append(Formatter::status(msg));
}

LinkStatus Connection::ok() {
  return this->append(Formatter::ok());
}

LinkStatus Connection::null() {
  return this->append(Formatter::null());
}

LinkStatus Connection::integer(int64_t number) {
  return this->append(Formatter::integer(number));
}

LinkStatus Connection::vector(const std::vector<std::string> &vec) {
  return this->append(Formatter::vector(vec));
}

LinkStatus Connection::scan(const std::string &marker, const std::vector<std::string> &vec) {
  return this->append(Formatter::scan(marker, vec));
}

LinkStatus Connection::flushPending(const std::string &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  while(!pending.empty()) {
    this->send(Formatter::err(msg));
    pending.pop();
  }
  return 1;
}

LinkStatus Connection::append(std::string &&raw) {
  std::lock_guard<std::mutex> lock(mtx);
  if(pending.empty()) return this->send(std::move(raw));

  // we're being blocked by a write, must queue
  PendingRequest req;
  req.rawResp = std::move(raw);
  pending.push(std::move(req));
  return 1;
}

LinkStatus Connection::appendReq(RedisDispatcher *dispatcher, RedisRequest &&req, LogIndex index) {
  std::lock_guard<std::mutex> lock(mtx);
  if(pending.empty() && index < 0) return this->send(dispatcher->dispatch(req, 0));

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

LinkStatus Connection::send(std::string &&raw) {
  if(!link) return 1;
  return link->Send(raw);
}

LogIndex Connection::dispatchPending(RedisDispatcher *dispatcher, LogIndex commitIndex) {
  std::lock_guard<std::mutex> lock(mtx);

  while(!pending.empty()) {
    PendingRequest &req = pending.front();
    if(commitIndex < req.index) {
      // the rest of the items are blocked, return new blocking index
      return req.index;
    }

    if(!req.rawResp.empty()) {
      this->send(std::move(req.rawResp));
    }
    else {
      this->send(dispatcher->dispatch(req.req, commitIndex));
    }

    pending.pop();
  }

  // no more pending requests
  return -1;
}
