// ----------------------------------------------------------------------
// File: RaftResilverer.hh
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

#ifndef __QUARKDB_RAFT_RESILVERER_H__
#define __QUARKDB_RAFT_RESILVERER_H__

#include <qclient/QClient.hh>
#include "utils/AssistedThread.hh"
#include "Common.hh"
#include "raft/RaftTalker.hh"

namespace quarkdb {

using ResilveringEventID = std::string;
using redisReplyPtr = qclient::redisReplyPtr;

enum class ResilveringState {
  INPROGRESS = 0,
  SUCCEEDED = 1,
  FAILED = 2
};

struct ResilveringStatus {
  ResilveringState state;
  std::string err;
};

class ShardDirectory; class RaftTrimmer;
class RaftTrimmingBlock; class RaftContactDetails;

class RaftResilverer {
public:
  RaftResilverer(ShardDirectory &directory, const RaftServer &target, const RaftContactDetails &cd, RaftTrimmer &trimmer);
  ~RaftResilverer();

  ResilveringStatus getStatus();
private:
  ShardDirectory &shardDirectory;
  RaftServer target;
  std::unique_ptr<RaftTrimmingBlock> trimmingBlock;

  RaftTalker talker;

  void setStatus(const ResilveringState &state, const std::string &err);
  ResilveringStatus status;
  std::mutex statusMtx;

  ResilveringEventID resilveringID = "super-random-string";

  AssistedThread mainThread;
  void main(ThreadAssistant &assistant);
  bool copyDirectory(const std::string &path, const std::string &prefix, std::string &err);

  void cancel(const std::string &reason);
  bool copyFile(const std::string &path, const std::string &prefix, std::string &err);
};

}

#endif
