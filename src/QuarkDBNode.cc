// ----------------------------------------------------------------------
// File: QuarkDBNode.cc
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

#include "StateMachine.hh"
#include "QuarkDBNode.hh"
#include "Version.hh"
#include "utils/FileUtils.hh"
#include "utils/ScopedAdder.hh"

#include <sys/stat.h>

using namespace quarkdb;

QuarkDBNode::~QuarkDBNode() {
  detach();
  qdb_info("Shutting down QuarkDB node.")
}

//------------------------------------------------------------------------------
// Make this node backend-less.
// It will no longer be able to service user requests.
//------------------------------------------------------------------------------

void QuarkDBNode::detach() {
  if(!attached) return;
  attached = false;

  qdb_event("Received request to detach this node. Spinning until all requests being dispatched (" << beingDispatched << ") have been processed..");
  while(beingDispatched != 0) ;
  qdb_info("Requests being dispatched: " << beingDispatched << ", it is now safe to detach.");

  if(raftgroup) {
    qdb_info("Shutting down the raft machinery.");
    delete raftgroup;
    raftgroup = nullptr;
  }
  else if(stateMachine) {
    qdb_info("Shutting down the state machine.");
    delete stateMachine;
    stateMachine = nullptr;

    delete dispatcher;
    dispatcher = nullptr;
  }

  delete shardDirectory;
  qdb_info("Backend has been detached from this quarkdb node.")
}

bool QuarkDBNode::attach(std::string &err) {
  if(attached) {
    err = "node already attached.";
    return false;
  }

  if(resilvering) {
    err = "cannot attach, resilvering in progress";
    return false;
  }

  shardDirectory = new ShardDirectory(configuration.getDatabase());

  if(configuration.getMode() == Mode::standalone) {
    stateMachine = shardDirectory->getStateMachine();
    dispatcher = new RedisDispatcher(*stateMachine);
  }
  else if(configuration.getMode() == Mode::raft) {
    raftgroup = new RaftGroup(*shardDirectory, configuration.getMyself(), timeouts);
    dispatcher = raftgroup->dispatcher();
    raftgroup->spinup();
  }
  else {
    qdb_throw("cannot determine configuration mode"); // should never happen
  }

  attached = true;
  return true;
}

void QuarkDBNode::cancelResilvering() {
  std::string path = SSTR(configuration.getDatabase() << "/resilvering-arena");

  struct stat sb;
  if(stat(path.c_str(), &sb) == 0) {
    qdb_critical("Deleting the contents of " << path << ", leftover from a failed resilvering");
    system(SSTR("rm -rf " << path).c_str());
  }

  resilvering = false;
}

QuarkDBNode::QuarkDBNode(const Configuration &config, const std::atomic<int64_t> &inFlight_, const RaftTimeouts &t)
: configuration(config), inFlight(inFlight_), timeouts(t) {
  cancelResilvering();

  std::string err;
  if(!attach(err)) qdb_critical(err);
}

LinkStatus QuarkDBNode::dispatch(Connection *conn, RedisRequest &req) {
  auto it = redis_cmd_map.find(req[0]);
  if(it == redis_cmd_map.end()) return conn->err(SSTR("unknown command " << quotes(req[0])));

  RedisCommand cmd = it->second.first;
  switch(cmd) {
    case RedisCommand::PING: {
      if(req.size() > 2) return conn->errArgs(req[0]);
      if(req.size() == 1) return conn->pong();

      return conn->string(req[1]);
    }
    case RedisCommand::DEBUG: {
      if(req.size() != 2) return conn->errArgs(req[0]);
      if(caseInsensitiveEquals(req[1], "segfault")) {
        qdb_critical("COMMITTING SEPPUKU ON CLIENT REQUEST: SEGV");
        *( (int*) nullptr) = 5;
      }

      if(caseInsensitiveEquals(req[1], "kill")) {
        qdb_critical("COMMITTING SEPPUKU ON CLIENT REQUEST: SIGKILL");
        system(SSTR("kill -9 " << getpid()).c_str());
        return conn->ok();
      }

      if(caseInsensitiveEquals(req[1], "terminate")) {
        qdb_critical("COMMITTING SEPPUKU ON CLIENT REQUEST: SIGTERM");
        system(SSTR("kill " << getpid()).c_str());
        return conn->ok();
      }

      return conn->err(SSTR("unknown argument '" << req[1] << "'"));
    }
    case RedisCommand::QUARKDB_INFO: {
      return conn->vector(this->info().toVector());
    }
    case RedisCommand::QUARKDB_STATS: {
      if(!attached) return conn->err("node is detached from any backends");
      return conn->vector(split(stateMachine->statistics(), "\n"));
    }
    case RedisCommand::QUARKDB_DETACH: {
      detach();
      return conn->ok();
    }
    case RedisCommand::QUARKDB_ATTACH: {
      std::string err;
      if(attach(err)) return conn->ok();
      return conn->err(err);
    }
    case RedisCommand::QUARKDB_START_RESILVERING: {
      if(resilvering) {
        cancelResilvering();
        return conn->err("resilvering was already in progress. Calling it off");
      }

      if(attached) {
        detach();
      }

      if(mkdir(SSTR(configuration.getDatabase() << "/resilvering-arena").c_str(), 0755) != 0) {
        return conn->err("could not create resilvering arena");
      }

      qdb_event("Entering resilvering mode. Re-attaching during resilvering is forbidden.");
      resilvering = true;
      return conn->ok();
    }
    case RedisCommand::QUARKDB_CANCEL_RESILVERING: {
      if(!resilvering) return conn->err("resilvering not in progress");
      cancelResilvering();
      return conn->ok();
    }
    case RedisCommand::QUARKDB_FINISH_RESILVERING: {
      if(!resilvering) return conn->err("resilvering not in progress");
      qdb_event("Received request to finish resilvering.");
      std::string oldSnapshots = SSTR(configuration.getDatabase() << "/old-checkpoint-" << TIME_NOW);

      std::string errMsg;
      if(mkdir(oldSnapshots.c_str(), 0755) != 0) {
        errMsg = SSTR("could not create old checkpoints dir: " << strerror(errno));
        qdb_critical(errMsg);
        return conn->err(errMsg);
      }

      system(SSTR("mv " << configuration.getDatabase() << "/raft-journal " << oldSnapshots).c_str());
      system(SSTR("mv " << configuration.getDatabase() << "/state-machine " << oldSnapshots).c_str());

      system(SSTR("mv " << configuration.getDatabase() << "/resilvering-arena/raft-journal " << configuration.getDatabase()).c_str());
      system(SSTR("mv " << configuration.getDatabase() << "/resilvering-arena/state-machine " << configuration.getDatabase()).c_str());

      resilvering = false;

      std::string err;
      if(attach(err)) {
        qdb_event("RESILVERING SUCCESSFUL! Continuing normally.");
        return conn->ok();
      }

      qdb_critical("Error when re-attaching after resilvering: " << err);
      cancelResilvering();
      return conn->err(err);
    }
    case RedisCommand::QUARKDB_RESILVERING_COPY_FILE: {
      if(!resilvering) return conn->err("resilvering not in progress, cannot copy file");
      if(req.size() != 3) return conn->errArgs(req[0]);

      std::string errMsg;
      std::string targetPath = SSTR(configuration.getDatabase() << "/resilvering-arena/" << req[1]);
      int fd;

      if(!mkpath(targetPath, 0755, errMsg)) {
        errMsg = "could not create path";
        goto error;
      }

      fd = creat(targetPath.c_str(), 0664);

      if(fd < 0) {
        errMsg = "could not create file";
        goto error;
      }

      if(write(fd, req[2].c_str(), req[2].size()) < 0) {
        errMsg = "could not write file";
        goto error;
      }

      if(close(fd) < 0) {
        errMsg = "could not close file";
        goto error;
      }

      qdb_info("RESILVERING: copied file " << req[1] << " (size: " << req[2].size() << ") successfully.");
      return conn->ok();
error:
      int localErrno = errno;
      qdb_critical("calling off resilvering, " << errMsg << ":" << strerror(localErrno));
      cancelResilvering();
      return conn->err(SSTR("resilvering called off, " << errMsg << ": " << strerror(localErrno)));
    }
    default: {
      if(!attached) return conn->err("node is detached from any backends");
      ScopedAdder<int64_t> adder(beingDispatched);
      if(!attached) return conn->err("node is detached from any backends");

      return dispatcher->dispatch(conn, req);
    }
  }
}

QuarkDBInfo QuarkDBNode::info() {
  return {attached, resilvering, configuration.getMode(), configuration.getDatabase(),
          STRINGIFY(VERSION_FULL), SSTR(ROCKSDB_MAJOR << "." << ROCKSDB_MINOR << "." << ROCKSDB_PATCH),
          inFlight};
}
