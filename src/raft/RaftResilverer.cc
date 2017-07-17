// ----------------------------------------------------------------------
// File: RaftResilverer.cc
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

#include "RaftResilverer.hh"
#include "RaftTrimmer.hh"
#include "../ShardDirectory.hh"
#include "../Utils.hh"
#include <dirent.h>
#include <fstream>
#include "../utils/Uuid.hh"
#include "../utils/DirectoryIterator.hh"

using namespace quarkdb;

static bool is_ok_response(std::future<redisReplyPtr> &fut) {
  std::future_status status = fut.wait_for(std::chrono::seconds(3));
  if(status != std::future_status::ready) {
    return false;
  }

  redisReplyPtr rep = fut.get();
  if(rep == nullptr) return false;

  if(rep->type != REDIS_REPLY_STATUS) return false;
  if(std::string(rep->str, rep->len) != "OK") return false;

  return true;
}

RaftResilverer::RaftResilverer(ShardDirectory &dir, const RaftServer &trg, const RaftClusterID &cid, RaftTrimmer *trim)
: shardDirectory(dir), target(trg), clusterID(cid), trimmer(trim), talker(target, clusterID) {

  resilveringID = generateUuid();

  if(trimmer) trimmer->resilveringInitiated();
  setStatus(ResilveringState::INPROGRESS, "");
  mainThread = AssistedThread(&RaftResilverer::main, this);
}

RaftResilverer::~RaftResilverer() {
  mainThread.join();
  if(trimmer) trimmer->resilveringOver();
}

ResilveringStatus RaftResilverer::getStatus() {
  std::lock_guard<std::mutex> lock(statusMtx);
  return status;
}

void RaftResilverer::setStatus(const ResilveringState &state, const std::string &err) {
  std::lock_guard<std::mutex> lock(statusMtx);
  status.state = state;
  status.err = err;

  if(status.state == ResilveringState::FAILED) {
    qdb_warn("Attempt to resilver a node has failed: " << status.err);
    cancel(status.err);
  }
}

void RaftResilverer::cancel(const std::string &reason) {
  // Fire and forget. The target should be able to automatically cancel failed
  // resilverings after some timeout, anyway.

  talker.resilveringCancel(resilveringID, reason);
}

bool RaftResilverer::copyDirectory(const std::string &target, const std::string &prefix, std::string &err) {
  qdb_info("Resilvering: Copying directory " << target << " under prefix '" << prefix << "' of remote target");

  DirectoryIterator dirIterator(target);

  struct dirent *entry;
  while( (entry = dirIterator.next()) ) {
    if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;

    std::string currentPath = SSTR(target << "/" << entry->d_name);
    std::string currentPrefix;

    if(prefix.empty()) {
      currentPrefix = SSTR(entry->d_name);
    }
    else {
      currentPrefix = SSTR(prefix << "/" << entry->d_name);
    }

    if(entry->d_type == DT_DIR) {
      if(!copyDirectory(currentPath, currentPrefix, err)) {
        return false;
      }
    }
    else {
      std::ifstream t(currentPath);
      std::stringstream buffer;
      buffer << t.rdbuf();

      std::future<redisReplyPtr> fut = talker.resilveringCopy(resilveringID, currentPrefix, buffer.str());
      if(!is_ok_response(fut)) return false;
    }
  }

  if(!dirIterator.ok()) {
    err = SSTR("copyDirectory failed, unable to iterate directory: " << dirIterator.err());
    return false;
  }

  return true;
}

void RaftResilverer::main(ThreadAssistant &assistant) {
  std::future<redisReplyPtr> fut = talker.resilveringStart(resilveringID);

  if(!is_ok_response(fut)) {
    setStatus(ResilveringState::FAILED, "Could not initiate resilvering");
    return;
  }

  std::string err;
  std::unique_ptr<ShardSnapshot> shardSnapshot = shardDirectory.takeSnapshot(resilveringID, err);

  if(shardSnapshot == nullptr || !err.empty()) {
    setStatus(ResilveringState::FAILED, "Could not create snapshot");
    return;
  }

  if(!copyDirectory(shardSnapshot->getPath(), "", err)) {
    setStatus(ResilveringState::FAILED, err);
    return;
  }

  fut = talker.resilveringFinish(resilveringID);
  if(!is_ok_response(fut)) {
    setStatus(ResilveringState::FAILED, err);
    return;
  }

  setStatus(ResilveringState::SUCCEEDED, "");
}
