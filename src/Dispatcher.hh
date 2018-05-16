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

#include "Common.hh"
#include "Link.hh"
#include "Commands.hh"
#include "Connection.hh"

namespace quarkdb {

class MultiOp;

class Dispatcher {
public:
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) = 0;
  RedisEncodedResponse handlePing(RedisRequest &req);
  virtual ~Dispatcher() {}
};

class StateMachine; class StagingArea;

using VecIterator = std::vector<std::string>::const_iterator;

class RedisDispatcher : public Dispatcher {
public:
  RedisDispatcher(StateMachine &rocksdb);
  virtual LinkStatus dispatch(Connection *conn, RedisRequest &req) override final;

  RedisEncodedResponse dispatch(RedisRequest &req, LogIndex commit);
  RedisEncodedResponse dispatch(MultiOp &multiOp, LogIndex commit, bool phantom);
private:
  RedisEncodedResponse handleMultiOp(RedisRequest &req, LogIndex commit);
  RedisEncodedResponse dispatchReadWrite(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse dispatchRead(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse dispatchWrite(StagingArea &stagingArea, RedisRequest &req);
  RedisEncodedResponse errArgs(RedisRequest &request);
  RedisEncodedResponse dispatchingError(RedisRequest &request, LogIndex commit);

  StateMachine &store;

  RedisEncodedResponse dispatchHGET(StagingArea &stagingArea, const std::string &key, const std::string &field);
  RedisEncodedResponse dispatchLHGET(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &hint);
  RedisEncodedResponse dispatchLHSET(StagingArea &stagingArea, const std::string &key, const std::string &field, const std::string &hint, const std::string &value);
  RedisEncodedResponse dispatchHDEL(StagingArea &stagingArea, const std::string &key, const VecIterator &start, const VecIterator &end);

};

}

#endif
