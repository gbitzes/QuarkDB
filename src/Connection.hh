//----------------------------------------------------------------------
// File: Connection.hh
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

#ifndef __QUARKDB_CONNECTION_H__
#define __QUARKDB_CONNECTION_H__

#include "Link.hh"
#include "RedisParser.hh"
#include "BufferedWriter.hh"
#include <queue>

#define OUTPUT_BUFFER_SIZE (16*1024)

namespace rocksdb {
  class Status;
}

namespace quarkdb {

//------------------------------------------------------------------------------
// Keeps track of a list of pending requests, which can optionally be related
// to a Connection.
//
// Why "optionally"? There's no guarantee that by the time a pending request
// is ready to be serviced, the connection will still be alive! The client
// might have disconnected in the meantime, even after issuing writes that
// have already been appended to the raft journal.
//------------------------------------------------------------------------------

class Connection;
class RedisDispatcher;
class PendingQueue {
public:
  PendingQueue(Connection *c) : conn(c) {}
  ~PendingQueue() {}

  void detachConnection() {
    std::lock_guard<std::mutex> lock(mtx);
    conn = nullptr;
  }

  LinkStatus flushPending(const std::string &msg);
  LinkStatus appendResponse(std::string &&raw);
  LinkStatus addPendingRequest(RedisDispatcher *dispatcher, RedisRequest &&req, LogIndex index = -1);
  LogIndex dispatchPending(RedisDispatcher *dispatcher, LogIndex commitIndex);
private:
  Connection *conn;
  std::mutex mtx;

  //----------------------------------------------------------------------------
  // Information about a pending request, which can be either a read or a write.
  // Every write corresponds to exactly one entry in the raft journal. Naturally,
  // we have to wait until it's committed before responding to the client.
  //
  // But why do reads need to wait, too? If a read request is made right after a
  // write with pipelining, we have to give the responses in the correct order,
  // so a read has to be queued until the write that's blocking us has finished.
  //
  // The queue will usually look like this:
  // write, read, read, read, write, read, read, read, write, write
  //
  // All read requests are being blocked by one or more writes, and each write
  // corresponds to a unique raft journal entry.
  //
  // Reads will be processed as soon as they aren't being blocked by a write. If
  // all a client does is read, the queue will not be used.
  //----------------------------------------------------------------------------

  struct PendingRequest {
    RedisRequest req;
    std::string rawResp; // if not empty, we're just storing a raw, pre-formatted response
    LogIndex index = -1; // the corresponding entry in the raft journal - only relevant for write requests
  };

  LogIndex lastIndex = -1;
  std::queue<PendingRequest> pending;
};

//------------------------------------------------------------------------------
// Keeps track of connection-specific state.
//------------------------------------------------------------------------------
class Dispatcher; class InFlightTracker;

class Connection {
public:
  Connection(Link *link);
  ~Connection();

  LinkStatus raw(std::string &&raw);
  LinkStatus moved(int64_t shardId, const RaftServer &location);
  LinkStatus err(const std::string &msg);
  LinkStatus errArgs(const std::string &cmd);
  LinkStatus pong();
  LinkStatus string(const std::string &str);
  LinkStatus fromStatus(const rocksdb::Status &status);
  LinkStatus status(const std::string &msg);
  LinkStatus ok();
  LinkStatus null();
  LinkStatus integer(int64_t number);
  LinkStatus vector(const std::vector<std::string> &vec);
  LinkStatus scan(const std::string &marker, const std::vector<std::string> &vec);

  bool raftAuthorization = false;
  LinkStatus processRequests(Dispatcher *dispatcher, const InFlightTracker &tracker);
  void setResponseBuffering(bool value);
  void flush();

  LinkStatus addPendingRequest(RedisDispatcher *dispatcher, RedisRequest &&req, LogIndex index = -1) {
    return pendingQueue->addPendingRequest(dispatcher, std::move(req), index);
  }

  LinkStatus flushPending(const std::string &msg) {
    return pendingQueue->flushPending(msg);
  }

  LogIndex dispatchPending(RedisDispatcher *dispatcher, LogIndex commitIndex) {
    return pendingQueue->dispatchPending(dispatcher, commitIndex);
  }

  std::shared_ptr<PendingQueue> getQueue() {
    return pendingQueue;
  }

  class FlushGuard {
  public:
    FlushGuard(Connection *c) : conn(c) { }
    ~FlushGuard() { if(conn) { conn->flush(); } }
  private:
    Connection *conn;
    BufferedWriter *writer;
  };
private:
  BufferedWriter writer;

  RedisRequest currentRequest;
  RedisParser parser;
  std::shared_ptr<PendingQueue> pendingQueue;
  friend class PendingQueue;
};


}

#endif
