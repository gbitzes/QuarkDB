//-----------------------------------------------------------------------
// File: Dispatcher.hh
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

#ifndef __QUARKDB_DISPATCHER_H__
#define __QUARKDB_DISPATCHER_H__

#include "WriteBatch.hh"
#include "Common.hh"
#include "Link.hh"
#include "Commands.hh"
#include "Connection.hh"

namespace quarkdb {

class MultiOp;

class Dispatcher {
public:
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) = 0;
  virtual LinkStatus dispatch(Connection *conn, MultiOp &multiOp) = 0;

  // Default implementation simply calls dispatch multiple times. Individual
  // dispatchers should override this with something more efficient.
  // TODO: remove default implementation once every dispatcher implements this

  virtual LinkStatus dispatch(Connection *conn, WriteBatch &batch) {
    LinkStatus lastStatus = 0;
    for(size_t i = 0; i < batch.requests.size(); i++) {
      lastStatus = this->dispatch(conn, batch.requests[i]);
    }
    return lastStatus;
  }

  RedisEncodedResponse handlePing(RedisRequest &req);
  virtual ~Dispatcher() {}
};

class StateMachine; class StagingArea;

class RedisDispatcher : public Dispatcher {
public:
  RedisDispatcher(StateMachine &rocksdb);
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;
  virtual LinkStatus dispatch(Connection *conn, WriteBatch &batch) override final;
  virtual LinkStatus dispatch(Connection *conn, MultiOp &multiOp) override final;

  RedisEncodedResponse dispatch(RedisRequest &req, LogIndex commit);
  RedisEncodedResponse dispatch(MultiOp &multiOp, LogIndex commit);
private:
  RedisEncodedResponse dispatchInternal(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse dispatchRead(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse dispatchWrite(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse errArgs(RedisRequest &request);
  RedisEncodedResponse dispatchingError(RedisRequest &request, LogIndex commit);

  StateMachine &store;
};

}

#endif
