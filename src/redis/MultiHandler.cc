// ----------------------------------------------------------------------
// File: MultiHandler.cc
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

#include "../utils/Macros.hh"
#include "MultiHandler.hh"
#include "../Dispatcher.hh"
#include "../Connection.hh"
using namespace quarkdb;

MultiHandler::MultiHandler() {
}

bool MultiHandler::active() const {
  return activated;
}

void MultiHandler::activate() {
  qdb_assert(!activated);
  activated = true;
}

LinkStatus MultiHandler::process(Dispatcher *dispatcher, Connection *conn, RedisRequest &req) {
  qdb_assert(activated || req.getCommand() == RedisCommand::MULTI);

  if(req.getCommand() == RedisCommand::DISCARD) {
    multiOp.clear();
    activated = false;
    return conn->ok();
  }

  if(req.getCommand() == RedisCommand::MULTI) {
    if(req.size() != 1u) {
      return conn->errArgs(req[0]);
    }

    if(activated) {
      return conn->err("MULTI calls can not be nested");
    }

    activated = true;
    return conn->ok();
  }

  if(req.getCommand() == RedisCommand::EXEC) {
    // Empty multi-exec block?
    if(multiOp.empty()) {
      activated = false;
      return conn->vector( {} );
    }

    RedisRequest fused;
    fused.emplace_back(multiOp.getFusedCommand());
    fused.emplace_back(multiOp.serialize());

    multiOp.clear();
    activated = false;

    return dispatcher->dispatch(conn, fused);
  }

  if(req.getCommandType() != CommandType::READ && req.getCommandType() != CommandType::WRITE) {
    return conn->err("Only reads and writes allowed within MULTI/EXEC blocks.");
  }

  // Queue
  multiOp.push_back(req);
  return conn->status("QUEUED");
}
