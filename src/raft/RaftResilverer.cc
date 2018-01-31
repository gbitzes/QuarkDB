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

class OkResponseVerifier {
public:
  OkResponseVerifier(std::future<redisReplyPtr> &&fut, size_t timeout = 15) {
    std::future_status status = fut.wait_for(std::chrono::seconds(timeout));
    if(status != std::future_status::ready) {
      error = SSTR("Timeout after " << timeout << " seconds");
      return;
    }

    redisReplyPtr rep = fut.get();
    if(rep == nullptr) {
      error = SSTR("Received nullptr response (should never happen)");
      return;
    }

    if(rep->type != REDIS_REPLY_STATUS) {
      error = SSTR("Unexpected response type: " << rep->type);
      return;
    }

    std::string response = std::string(rep->str, rep->len);
    if(response != "OK") {
      error = SSTR("Unexpected response: " << response);
      return;
    }
  }

  bool ok() {
    return error.empty();
  }

  std::string err() {
    return error;
  }

private:
  std::string error;
};

RaftResilverer::RaftResilverer(ShardDirectory &dir, const RaftServer &trg, const RaftClusterID &cid, const RaftTimeouts &timeouts, RaftTrimmer &trimmer)
: shardDirectory(dir), target(trg), clusterID(cid),
  trimmingBlock(new RaftTrimmingBlock(trimmer, 0)),
  talker(target, clusterID, timeouts) {

  resilveringID = generateUuid();
  setStatus(ResilveringState::INPROGRESS, "");
  mainThread.reset(&RaftResilverer::main, this);
}

RaftResilverer::~RaftResilverer() {
  mainThread.join();
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
    qdb_critical("Attempt to resilver " << target.toString() << " has failed: " << status.err);
    cancel(status.err);
  }
  else if(status.state == ResilveringState::SUCCEEDED) {
    qdb_event("Target " << target.toString() << " has been successfully resilvered.");
  }
}

void RaftResilverer::cancel(const std::string &reason) {
  // Fire and forget. The target should be able to automatically cancel failed
  // resilverings after some timeout, anyway.

  talker.resilveringCancel(resilveringID, reason);
}

bool RaftResilverer::copyFile(const std::string &path, const std::string &prefix, std::string &err) {
  std::ifstream t(path);
  std::stringstream buffer;
  buffer << t.rdbuf();

  OkResponseVerifier verifier(talker.resilveringCopy(resilveringID, prefix, buffer.str()));
  if(!verifier.ok()) {
    err = SSTR("Error when coping " << path << ": " << verifier.err());
  }

  return verifier.ok();
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
      if(!copyFile(currentPath, currentPrefix, err)) {
        return false;
      }
    }
  }

  if(!dirIterator.ok()) {
    err = SSTR("copyDirectory failed, unable to iterate directory: " << dirIterator.err());
    return false;
  }

  return true;
}

void RaftResilverer::main(ThreadAssistant &assistant) {
  OkResponseVerifier verifier(talker.resilveringStart(resilveringID));
  if(!verifier.ok()) {
    setStatus(ResilveringState::FAILED, SSTR("Could not initiate resilvering: " << verifier.err()));
    return;
  }

  std::string err;
  std::unique_ptr<ShardSnapshot> shardSnapshot = shardDirectory.takeSnapshot(resilveringID, err);

  if(shardSnapshot == nullptr || !err.empty()) {
    setStatus(ResilveringState::FAILED, SSTR("Could not create snapshot: " << err));
    return;
  }

  if(!copyDirectory(shardSnapshot->getPath(), "", err)) {
    setStatus(ResilveringState::FAILED, err);
    return;
  }

  verifier = OkResponseVerifier(talker.resilveringFinish(resilveringID), 60);
  if(!verifier.ok()) {
    setStatus(ResilveringState::FAILED, SSTR("Error when finishing resilvering: " << verifier.err()));
    return;
  }

  setStatus(ResilveringState::SUCCEEDED, "");
}
