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

#ifndef QUARKDB_DISPATCHER_H
#define QUARKDB_DISPATCHER_H

#include "Common.hh"
#include "Link.hh"
#include "Commands.hh"
#include "Connection.hh"

namespace quarkdb {

class Transaction;

class Dispatcher {
public:
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) = 0;
  virtual LinkStatus dispatch(Connection *conn, Transaction &multiOp) = 0;
  virtual void notifyDisconnect(Connection *conn) = 0;

  RedisEncodedResponse handlePing(RedisRequest &req);
  RedisEncodedResponse handleConversion(RedisRequest &req);
  virtual ~Dispatcher() {}
};

class StateMachine; class StagingArea;
class Publisher;

class RedisDispatcher : public Dispatcher {
public:
  RedisDispatcher(StateMachine &rocksdb, Publisher &publisher);
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;
  virtual LinkStatus dispatch(Connection *conn, Transaction &req) override final;
  virtual void notifyDisconnect(Connection *conn) override final {}

  RedisEncodedResponse dispatch(RedisRequest &req, LogIndex commit);
  RedisEncodedResponse dispatch(Transaction &transaction, LogIndex commit);
private:
  RedisEncodedResponse dispatchReadOnly(StagingArea &stagingArea, Transaction &transaction);
  RedisEncodedResponse dispatch(StagingArea &stagingArea, Transaction &transaction);
  RedisEncodedResponse dispatchReadWrite(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse dispatchReadWriteAndCommit(RedisRequest &req, LogIndex commit);

  RedisEncodedResponse dispatchRead(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse dispatchWrite(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse errArgs(RedisRequest &request);
  RedisEncodedResponse dispatchingError(RedisRequest &request, LogIndex commit);

  RedisEncodedResponse dispatchHGET(StagingArea &stagingArea, std::string_view key, std::string_view field);
  RedisEncodedResponse dispatchLHGET(StagingArea &stagingArea, std::string_view key,  std::string_view field, std::string_view hint);
  RedisEncodedResponse dispatchLHSET(StagingArea &stagingArea, std::string_view key, std::string_view field, std::string_view hint, std::string_view value);
  RedisEncodedResponse dispatchHDEL(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end);
  RedisEncodedResponse dispatchLHDEL(StagingArea &stagingArea, std::string_view key, const ReqIterator &start, const ReqIterator &end);

  StateMachine &store;
  Publisher &publisher;
};

}

#endif
