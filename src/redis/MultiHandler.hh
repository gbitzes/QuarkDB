// ----------------------------------------------------------------------
// File: MultiHandler.hh
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

#ifndef QUARKDB_REDIS_MULTIHANDLER_H
#define QUARKDB_REDIS_MULTIHANDLER_H

#include "MultiOp.hh"
#include "RedisEncodedResponse.hh"

namespace quarkdb {

class Dispatcher; class RedisRequest; class Connection;
using LinkStatus = int;

class MultiHandler {
public:
  MultiHandler();

  bool active() const;
  LinkStatus process(Dispatcher *dispatcher, Connection *conn, RedisRequest &req);
  void activatePhantom();
  size_t size() const;
  bool isPhantom() const { return activated && phantom; }
  LinkStatus finalizePhantomTransaction(Dispatcher *dispatcher, Connection *conn);

private:
  MultiOp multiOp;
  bool activated = false;
  bool phantom = false;
};

}

#endif
