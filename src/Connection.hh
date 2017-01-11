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
#include "RocksDB.hh"
#include <queue>

namespace quarkdb {

//------------------------------------------------------------------------------
// Keeps track of connection-specific state.
//
// The proper OOP solution would be to separate the raft-specific parts into
// their own class which inherits from this one, but since we only have two
// specializations (raft, and non-raft) let's not make things more complicated
// than they need to be. The raft parts are simply not used in the single-node
// case.
//------------------------------------------------------------------------------

class RedisDispatcher;
class Connection {
public:
  Connection(Link *link);
  ~Connection();

  LinkStatus raw(std::string &&raw);
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

  LinkStatus flushPending(const std::string &msg);

  LinkStatus appendReq(RedisDispatcher *dispatcher, RedisRequest &&req, LogIndex index = -1);
  LogIndex dispatchPending(RedisDispatcher *dispatcher, LogIndex commitIndex);

  bool raftAuthorization = false;
private:
  LinkStatus send(std::string && raw);
  LinkStatus append(std::string &&raw);

  Link *link = nullptr;
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
    std::string rawResp; // if not empty, we're just storing a raw, per-formatted response
    LogIndex index = -1; // the corresponding entry in the raft journal - only relevant for write requests
  };

  LogIndex lastIndex = -1;
  std::queue<PendingRequest> pending;
};

}

#endif
