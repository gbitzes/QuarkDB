// ----------------------------------------------------------------------
// File: AuthenticationDispatcher.cc
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
#include "AuthenticationDispatcher.hh"
#include "../Formatter.hh"
using namespace quarkdb;

AuthenticationDispatcher::AuthenticationDispatcher(const std::string &secr)
: secret(secr) {
  if(secret.size() < 32u && !secret.empty()) {
    qdb_throw("Password is too small, minimum length is 32");
  }

}

RedisEncodedResponse AuthenticationDispatcher::dispatch(const RedisRequest &req, bool &authorized) {
  authorized = secret.empty();

  switch(req.getCommand()) {
    case RedisCommand::AUTH: {
      if(req.size() != 2u) return Formatter::errArgs(req[0]);
      if(secret.empty()) return Formatter::err("Client sent AUTH, but no password is set");
      qdb_warn("A client used AUTH, which is highly discouraged.");

      if(secret != req[1]) {
        qdb_warn("A password attempt was made with an invalid password");
        return Formatter::err("invalid password");
      }

      authorized = true;
      return Formatter::ok();
    }
    default: {
      qdb_throw("Internal dispatching error for command " << req.toPrintableString());
    }
  }
}

LinkStatus AuthenticationDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  return conn->raw(dispatch(req, conn->authorization));
}
