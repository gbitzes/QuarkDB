// ----------------------------------------------------------------------
// File: XrdRedisProtocol.cc
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

#include "XrdVersion.hh"
#include "XrdRedisProtocol.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "raft/RaftDispatcher.hh"

#include <stdlib.h>
#include <algorithm>

using namespace quarkdb;


//------------------------------------------------------------------------------
// Globals
//------------------------------------------------------------------------------

Configuration XrdRedisProtocol::configuration;
XrdSysError XrdRedisProtocol::eDest(0, "redis");

const char *XrdRedisTraceID = "XrdRedis";
XrdOucTrace *XrdRedisTrace = 0;
XrdBuffManager *XrdRedisProtocol::bufferManager = 0;
RocksDB *XrdRedisProtocol::rocksdb = 0;
Dispatcher *XrdRedisProtocol::dispatcher = 0;
RaftJournal *XrdRedisProtocol::journal = 0;
RaftState *XrdRedisProtocol::state = 0;
RaftClock *XrdRedisProtocol::raftClock = 0;
RaftDirector *XrdRedisProtocol::director = 0;

//------------------------------------------------------------------------------
// XrdRedisProtocol class
//------------------------------------------------------------------------------

XrdRedisProtocol::XrdRedisProtocol()
: XrdProtocol("Redis protocol handler") {
  Reset();
}

int XrdRedisProtocol::Process(XrdLink *lp) {
  if(!link) link = new Link(lp);
  if(!parser) parser = new RedisParser(link, bufferManager);
  if(!conn) conn = new Connection(link);

  while(true) {
    LinkStatus status = parser->fetch(currentRequest);
    if(status == 0) return 1;     // slow link
    if(status < 0) return status; // error

    dispatcher->dispatch(conn, currentRequest);
  }
}

XrdProtocol* XrdRedisProtocol::Match(XrdLink *lp) {
  XrdRedisProtocol *rp = new XrdRedisProtocol();
  return rp;
}

void XrdRedisProtocol::Reset() {
  if(parser) {
    delete parser;
    parser = nullptr;
  }
  if(conn) {
    delete conn;
    conn = nullptr;
  }
  if(link) {
    delete link;
    link = nullptr;
  }
}

void XrdRedisProtocol::Recycle(XrdLink *lp,int consec,const char *reason) {

}

int XrdRedisProtocol::Stats(char *buff, int blen, int do_sync) {
  return 0;
}

void XrdRedisProtocol::DoIt() {

}

int XrdRedisProtocol::Configure(char *parms, XrdProtocol_Config * pi) {
  bufferManager = pi->BPool;
  eDest.logger(pi->eDest->logger());

  char* rdf = (parms && *parms ? parms : pi->ConfigFN);
  bool success = Configuration::fromFile(rdf, configuration);
  if(!success) return 0;

  rocksdb = new RocksDB(configuration.getDB());

  if(configuration.getMode() == Mode::rocksdb) {
    dispatcher = new RedisDispatcher(*rocksdb);
  }
  else if(configuration.getMode() == Mode::raft) {
    if(pi->Port != configuration.getMyself().port) {
      std::cerr << "configuration error: xrootd listening port doesn't match redis.myself" << std::endl;
      return 0;
    }

    journal = new RaftJournal(configuration.getRaftLog());
    state = new RaftState(*journal, configuration.getMyself());
    raftClock = new RaftClock(defaultTimeouts);
    RaftDispatcher *raftdispatcher = new RaftDispatcher(*journal, *rocksdb, *state, *raftClock);
    dispatcher = raftdispatcher;
    director = new RaftDirector(*raftdispatcher, *journal, *state, *raftClock);
  }
  else {
    qdb_throw("cannot determine configuration mode"); // should never happen
  }

  return 1;
}

XrdRedisProtocol::~XrdRedisProtocol() {
  Reset();
}
