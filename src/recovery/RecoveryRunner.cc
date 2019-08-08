// ----------------------------------------------------------------------
// File: RecoveryRunner.cc
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

#include "RecoveryRunner.hh"
#include <rocksdb/sst_dump_tool.h>

using namespace quarkdb;

RecoveryRunner::RecoveryRunner(const std::string &path, int port)
: editor(path), dispatcher(editor), poller(port, &dispatcher)  {
  qdb_event("RECOVERY MODE is now active: Issue requests to port " << port << " through redis-cli.");
  qdb_info("\nUseful commands: \n"
  "  RECOVERY_GET, RECOVERY_SET, RECOVERY_DEL:\n"
  "    Note that these are very different beasts than the traditional GET, SET, DEL offered by QuarkDB.\n"
  "    These hit directly the rocksdb keys, without a KeyDescriptor or anything else in the middle. \n\n"
  "    This makes it possible to change low-level details (ie commit-index, last-applied, format, nodes),\n"
  "    but you better know what you're doing.\n\n"
  "  RECOVERY-INFO:\n"
  "    Displays values for all important internal values, as defined in storage/KeyConstants.hh.\n"
  "    Journals and state machines have different subsets of these! It's completely normal (and expected)\n"
  "    that a state machine does not have contain kJournal_*, for example.\n"
  );

  qdb_info("Issue requests to port " << port << " through redis-cli.");
}

RedisEncodedResponse RecoveryRunner::issueOneOffCommand(const std::string &path, RedisRequest &req) {
  RecoveryEditor editor(path);
  RecoveryDispatcher dispatcher(editor);
  return dispatcher.dispatch(req);
}

void RecoveryRunner::DumpTool(int argc, char** argv) {
  rocksdb::SSTDumpTool tool;
  tool.Run(argc, argv);
}

