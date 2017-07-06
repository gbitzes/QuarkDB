// ----------------------------------------------------------------------
// File: XrdQuarkDB.cc
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
#include "XrdQuarkDB.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "raft/RaftDispatcher.hh"
#include "utils/ScopedAdder.hh"

#include <stdlib.h>
#include <algorithm>

using namespace quarkdb;


//------------------------------------------------------------------------------
// Globals
//------------------------------------------------------------------------------

XrdSysError XrdQuarkDB::eDest(0, "quarkdb");

const char *XrdQuarkDBID = "XrdQuarkDB";
XrdOucTrace *XrdQuarkDBTrace = 0;
QuarkDBNode *XrdQuarkDB::quarkdbNode = 0;
std::atomic<bool> XrdQuarkDB::inShutdown {false};
std::atomic<int64_t> XrdQuarkDB::inFlight {0};
EventFD XrdQuarkDB::shutdownFD;

//------------------------------------------------------------------------------
// Shutdown mechanism. Here's how it works.
// The signal handler sets inShutdown and notifies shutdownMonitor. Since we can
// only call signal-safe functions there, using a condition variable is not
// safe. write() is signal-safe, so let's use an eventfd.
//
// After inShutdown is set, all new requests are rejected, and we wait until
// all requests currently in flight are completed before deleting the main node.
//------------------------------------------------------------------------------

void XrdQuarkDB::shutdownMonitor() {
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
// XrdQuarkDB class
//------------------------------------------------------------------------------

XrdQuarkDB::XrdQuarkDB(bool tls)
: XrdProtocol("Redis protocol handler") {
  Reset();

  tlsconfig.active = tls;
  if(tls) {
    tlsconfig.certificatePath = quarkdbNode->getConfiguration().getCertificatePath();
    tlsconfig.keyPath = quarkdbNode->getConfiguration().getKeyPath();
  }
}

int XrdQuarkDB::Process(XrdLink *lp) {
  if(inShutdown) { return -1; }
  ScopedAdder<int64_t> adder(inFlight);

  // TODO log client DN
  if(!link && tlsconfig.active) qdb_info("handling TLS connection. Security is intensifying");
  if(!link) link = new Link(lp, tlsconfig);
  if(!conn) conn = new Connection(link);

  LinkStatus status = conn->processRequests(quarkdbNode, inShutdown);
  if(inShutdown) return -1;
  return status;
}

XrdProtocol* XrdQuarkDB::Match(XrdLink *lp) {
  char buffer[2];

  // Peek at the first bytes of data
  int dlen = lp->Peek(buffer, (int) sizeof (buffer), 10000);
  if(dlen <= 0) return nullptr;

  if(buffer[0] != '*') {
    // This is probably a TLS connection. Reject if there's no certificate
    // configured.
    if(quarkdbNode->getConfiguration().getCertificatePath().empty()) {
      return nullptr;
    }

    return new XrdQuarkDB(true);
  }

  return new XrdQuarkDB(false); // TLS not enabled
}

void XrdQuarkDB::Reset() {
  if(conn) {
    delete conn;
    conn = nullptr;
  }
  if(link) {
    delete link;
    link = nullptr;
  }
}

void XrdQuarkDB::Recycle(XrdLink *lp,int consec,const char *reason) {

}

int XrdQuarkDB::Stats(char *buff, int blen, int do_sync) {
  return 0;
}

void XrdQuarkDB::DoIt() {

}

static void handle_sigint(int sig) {
  XrdQuarkDB::inShutdown = true;
  XrdQuarkDB::shutdownFD.notify();
}

int XrdQuarkDB::Configure(char *parms, XrdProtocol_Config * pi) {
  eDest.logger(pi->eDest->logger());

  char* rdf = (parms && *parms ? parms : pi->ConfigFN);

  Configuration configuration;
  bool success = Configuration::fromFile(rdf, configuration);
  if(!success) return 0;

  if(configuration.getMode() == Mode::raft && pi->Port != configuration.getMyself().port) {
    qdb_throw("configuration error: xrootd listening port doesn't match redis.myself");
    return 0;
  }

  quarkdbNode = new QuarkDBNode(configuration, inFlight, defaultTimeouts);
  std::thread(&XrdQuarkDB::shutdownMonitor).detach();
  signal(SIGINT, handle_sigint);
  signal(SIGTERM, handle_sigint);
  return 1;
}

XrdQuarkDB::~XrdQuarkDB() {
  Reset();
}
