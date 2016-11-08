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
using namespace quarkdb;

// a "devnull" connection
Connection::Connection() { }

Connection::Connection(Link *l)
: link(l) {
}

Connection::~Connection() {
  flushPending("connection shutting down");
}

LinkStatus Connection::err(const std::string &msg) {
  if(!link) return 1;
  return Response::err(link, msg);
}

LinkStatus Connection::errArgs(const std::string &cmd) {
  if(!link) return 1;
  return Response::errArgs(link, cmd);
}

LinkStatus Connection::pong() {
  if(!link) return 1;
  return Response::pong(link);
}

LinkStatus Connection::string(const std::string &str) {
  if(!link) return 1;
  return Response::string(link, str);
}

LinkStatus Connection::fromStatus(const rocksdb::Status &status) {
  if(!link) return 1;
  return Response::fromStatus(link, status);
}

LinkStatus Connection::ok() {
  if(!link) return 1;
  return Response::ok(link);
}

LinkStatus Connection::null() {
  if(!link) return 1;
  return Response::null(link);
}

LinkStatus Connection::integer(int64_t number) {
  if(!link) return 1;
  return Response::integer(link, number);
}

LinkStatus Connection::vector(const std::vector<std::string> &vec) {
  if(!link) return 1;
  return Response::vector(link, vec);
}

LinkStatus Connection::scan(const std::string &marker, const std::vector<std::string> &vec) {
  if(!link) return 1;
  return Response::scan(link, marker, vec);
}

LinkStatus Connection::flushPending(const std::string &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  while(!pending.empty()) {
    this->err(msg);
    pending.pop();
  }
  return 1;
}

LinkStatus Connection::appendError(const std::string &msg) {
  std::lock_guard<std::mutex> lock(mtx);
  if(pending.empty()) return this->err(msg);

  // we're being blocked by a write, must queue
  PendingRequest req;
  req.error = msg;
  pending.push(std::move(req));
  return 1;
}

LinkStatus Connection::appendReq(Dispatcher *dispatcher, RedisRequest &&req, LogIndex index) {
  std::lock_guard<std::mutex> lock(mtx);
  if(pending.empty() && index < 0) return dispatcher->dispatch(this, req);

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

LogIndex Connection::dispatchPending(Dispatcher *dispatcher, LogIndex commitIndex) {
  std::lock_guard<std::mutex> lock(mtx);

  while(!pending.empty()) {
    PendingRequest &req = pending.front();
    if(commitIndex < req.index) {
      // the rest of the items are blocked, return new blocking index
      return req.index;
    }

    if(!req.error.empty()) {
      this->err(req.error);
    }
    else {
      dispatcher->dispatch(this, req.req, commitIndex);
    }

    pending.pop();
  }

  // no more pending requests
  return -1;
}
