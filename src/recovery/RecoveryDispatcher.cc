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
#include "../utils/CommandParsing.hh"
#include "../utils/IntToBinaryString.hh"
#include "../storage/KeyConstants.hh"
#include "../raft/RaftMembers.hh"
#include "../utils/CommandParsing.hh"
using namespace quarkdb;

RecoveryDispatcher::RecoveryDispatcher(RecoveryEditor &ed) : editor(ed) {
}

LinkStatus RecoveryDispatcher::dispatch(Connection *conn, Transaction &tx) {
  qdb_throw("Transactions not supported in RecoveryDispatcher");
}

LinkStatus RecoveryDispatcher::dispatch(Connection *conn, RedisRequest &req) {
  return conn->raw(dispatch(req));
}

RedisEncodedResponse RecoveryDispatcher::dispatch(RedisRequest &request) {
  switch(request.getCommand()) {
    case RedisCommand::CONVERT_STRING_TO_INT:
    case RedisCommand::CONVERT_INT_TO_STRING: {
      return handleConversion(request);
    }
    default: {
      // no-op, continue
    }
  }

  if(request.getCommandType() != CommandType::RECOVERY) {
    std::string msg = SSTR("unable to dispatch command " << quotes(request[0]) << " - remember we're running in recovery mode, not all operations are available");
    qdb_warn(msg);
    return Formatter::err(msg);
  }

  switch(request.getCommand()) {
    case RedisCommand::RECOVERY_GET: {
      if(request.size() != 2) return Formatter::errArgs(request[0]);

      std::string value;
      rocksdb::Status st = editor.get(request[1], value);
      if(!st.ok()) return Formatter::fromStatus(st);
      return Formatter::string(value);
    }
    case RedisCommand::RECOVERY_SET: {
      if(request.size() != 3) return Formatter::errArgs(request[0]);
      return Formatter::fromStatus(editor.set(request[1], request[2]));
    }
    case RedisCommand::RECOVERY_DEL: {
      if(request.size() != 2) return Formatter::errArgs(request[0]);
      return Formatter::fromStatus(editor.del(request[1]));
    }
    case RedisCommand::RECOVERY_INFO: {
      if(request.size() != 1) return Formatter::errArgs(request[0]);
      return Formatter::vector(editor.retrieveMagicValues());
    }
    case RedisCommand::RECOVERY_FORCE_RECONFIGURE_JOURNAL: {
      if(request.size() != 3) return Formatter::errArgs(request[0]);

      RaftMembers members;
      if(!members.parse(request[1])) {
        return Formatter::err("cannot parse new members");
      }

      std::string clusterID;
      rocksdb::Status st = editor.get(KeyConstants::kJournal_ClusterID, clusterID);

      if(!st.ok()) {
        return Formatter::err(SSTR("unable to retrieve clusterID, status " << st.ToString() << " - are you sure this is a journal?"));
      }

      if(clusterID == request[2]) {
        return Formatter::err("when force reconfiguring, new clusterID must be different than old one");
      }

      // All checks are clear, proceed
      qdb_assert(editor.set(KeyConstants::kJournal_ClusterID, request[2]).ok());

      qdb_assert(editor.set(KeyConstants::kJournal_Members, request[1]).ok());
      qdb_assert(editor.set(KeyConstants::kJournal_MembershipEpoch, intToBinaryString(0)).ok());

      editor.del(KeyConstants::kJournal_PreviousMembers);
      editor.del(KeyConstants::kJournal_PreviousMembershipEpoch);

      return Formatter::ok();
    }
    case RedisCommand::RECOVERY_SCAN: {
      if(request.size() < 2) return Formatter::errArgs(request[0]);

      ScanCommandArguments args = parseScanCommand(request.begin()+1, request.end());
      if(!args.error.empty()) {
        return Formatter::err(args.error);
      }

      std::string nextCursor;
      std::vector<std::string> results;
      editor.scan(args.cursor, args.count, nextCursor, results);

      if(nextCursor == "") nextCursor = "0";
      else nextCursor = "next:" + nextCursor;
      return Formatter::scan(nextCursor, results);
    }
    default: {
      qdb_throw("should never reach here");
    }
  }
}
