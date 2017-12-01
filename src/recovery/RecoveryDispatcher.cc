// ----------------------------------------------------------------------
// File: RecoveryDispatcher.cc
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

#include "RecoveryDispatcher.hh"
#include "../Formatter.hh"
using namespace quarkdb;

RecoveryDispatcher::RecoveryDispatcher(RecoveryEditor &ed) : editor(ed) {
}

LinkStatus RecoveryDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  return conn->raw(dispatch(req));
}

RedisEncodedResponse RecoveryDispatcher::dispatch(RedisRequest &request) {
  switch(request.getCommand()) {
    case RedisCommand::GET: {
      if(request.size() != 2) return Formatter::errArgs(request[0]);

      std::string value;
      rocksdb::Status st = editor.get(request[1], value);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::SET: {
      if(request.size() != 3) return Formatter::errArgs(request[0]);
      return Formatter::fromStatus(editor.set(request[1], request[2]));
    }
    case RedisCommand::DEL: {
      if(request.size() != 2) return Formatter::errArgs(request[0]);
      return Formatter::fromStatus(editor.del(request[1]));
    }
    case RedisCommand::RECOVERY_INFO: {
      if(request.size() != 1) return Formatter::errArgs(request[0]);
      return Formatter::vector(editor.retrieveMagicValues());
    }
    default: {
      std::string msg = SSTR("unable to dispatch command " << quotes(request[0]) << " - remember we're running in recovery mode, not all operations are available");
      qdb_warn(msg);
      return Formatter::err(msg);
    }
  }
}
