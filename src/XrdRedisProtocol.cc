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

XrdSysError XrdRedisProtocol::eDest(0, "redis");

const char *XrdRedisTraceID = "XrdRedis";
XrdOucTrace *XrdRedisTrace = 0;
XrdBuffManager *XrdRedisProtocol::bufferManager = 0;
QuarkDBNode *XrdRedisProtocol::quarkdbNode = 0;
std::atomic<bool> XrdRedisProtocol::inShutdown {false};
std::atomic<int64_t> XrdRedisProtocol::inFlight {0};
EventFD XrdRedisProtocol::shutdownFD;

//------------------------------------------------------------------------------
// Shutdown mechanism. Here's how it works.
// The signal handler sets inShutdown and notifies shutdownMonitor. Since we can
// only call signal-safe functions there, using a condition variable is not
// safe. write() is signal-safe, so let's use an eventfd.
//
// After inShutdown is set, all new requests are rejected, and we wait until
// all requests currently in flight are completed before deleting the main node.
//------------------------------------------------------------------------------

void XrdRedisProtocol::shutdownMonitor() {
  while(!inShutdown) {
    shutdownFD.wait();
  }

  qdb_event("Received request to shut down. Spinning until all requests in flight (" << inFlight << ") have been processed..");

  while(inFlight != 0) ;
  qdb_info("Requests in flight: " << inFlight << ", it is now safe to shut down.");

  delete quarkdbNode;

  qdb_event("SHUTTING DOWN");
  std::exit(0);
}

//------------------------------------------------------------------------------
// XrdRedisProtocol class
//------------------------------------------------------------------------------

XrdRedisProtocol::XrdRedisProtocol()
: XrdProtocol("Redis protocol handler") {
  Reset();
}

int XrdRedisProtocol::Process(XrdLink *lp) {
  if(inShutdown) { return -1; }
  ScopedAdder<int64_t> adder(inFlight);
  if(inShutdown) { return -1; }

  if(!link) link = new Link(lp);
  if(!parser) parser = new RedisParser(link, bufferManager);
  if(!conn) conn = new Connection(link);

  while(true) {
    LinkStatus status = parser->fetch(currentRequest);
    if(status == 0) return 1;     // slow link
    if(status < 0) return status; // error

    quarkdbNode->dispatch(conn, currentRequest);
  }
}

XrdProtocol* XrdRedisProtocol::Match(XrdLink *lp) {
  char buffer[4];

  // Peek at the first bytes of data
  int dlen = lp->Peek(buffer, (int) sizeof (buffer), 10000);
  if(dlen <= 0) return nullptr;
  if(buffer[0] != '*') return nullptr;

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

static void handle_sigint(int sig) {
  XrdRedisProtocol::inShutdown = true;
  XrdRedisProtocol::shutdownFD.notify();
}

int XrdRedisProtocol::Configure(char *parms, XrdProtocol_Config * pi) {
  bufferManager = pi->BPool;
  eDest.logger(pi->eDest->logger());

  char* rdf = (parms && *parms ? parms : pi->ConfigFN);

  Configuration configuration;
  bool success = Configuration::fromFile(rdf, configuration);
  if(!success) return 0;

  if(configuration.getMode() == Mode::raft && pi->Port != configuration.getMyself().port) {
    std::cerr << "configuration error: xrootd listening port doesn't match redis.myself" << std::endl;
    return 0;
  }

  quarkdbNode = new QuarkDBNode(configuration, bufferManager, inFlight);
  std::thread(&XrdRedisProtocol::shutdownMonitor).detach();
  signal(SIGINT, handle_sigint);
  signal(SIGTERM, handle_sigint);
  return 1;
}

XrdRedisProtocol::~XrdRedisProtocol() {
  Reset();
}
