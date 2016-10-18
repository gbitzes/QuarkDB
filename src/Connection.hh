//----------------------------------------------------------------------
// File: RedisConnection.hh
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
#include "Response.hh"
#include <queue>

namespace quarkdb {

//------------------------------------------------------------------------------
// Information about a pending request, which can be either a read or a write.
// Every write corresponds to exactly one entry in the raft journal. Naturally,
// we have to wait until it's committed before responding to the client.
//
// But why do reads need to wait, too? If a read request is made right after a
// write with pipelining, we have to give the responses in the correct order,
// so a read has to be queued until the write that's blocking us has finished.
//
// The queue will usually look like this:
// write2, read2, read2, read2, write3, read3, read3, read3, write4, write5
//
// All readN requests are being blocked by one or more writes, and all writes
// correspond to a single raft journal entry.
//
// Reads will be processed as soon as they aren't being blocked by a write. If
// all a client does is read, the queue will not be used.
//------------------------------------------------------------------------------

struct PendingRequest {
  bool write; // this is a write request that must go through the raft log
  RedisRequest req; // the contents of the request
  LogIndex index; // the index of the *last* write request that we've observed.
};

//------------------------------------------------------------------------------
// Keeps track of connection-specific state.
//
// The proper OOP solution would be to separate the raft-specific parts into
// their own class which inherits from this one, but since we only have two
// specializations (raft, and non-raft) let's not make things more complicated
// than they need to be. The raft parts are simply not used in the single-node
// case.
//------------------------------------------------------------------------------

class Connection {
public:
  Connection(Link *link);
  ~Connection();

  LinkStatus err(const std::string &msg);
  LinkStatus errArgs(const std::string &cmd);
  LinkStatus pong();
  LinkStatus string(const std::string &str);
  LinkStatus fromStatus(const rocksdb::Status &status);
  LinkStatus ok();
  LinkStatus null();
  LinkStatus integer(int64_t number);
  LinkStatus vector(const std::vector<std::string> &vec);
  LinkStatus scan(const std::string &marker, const std::vector<std::string> &vec);

  bool raftAuthorization = false;
  std::queue<PendingRequest> pending;
private:
  Link *link;

};


}

#endif
