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

//------------------------------------------------------------------------------
// XrdRedisProtocol class
//------------------------------------------------------------------------------

XrdRedisProtocol::XrdRedisProtocol()
: XrdProtocol("Redis protocol handler") {
  Reset();
}

int XrdRedisProtocol::Process(XrdLink *lp) {
  return 0;
}

XrdProtocol* XrdRedisProtocol::Match(XrdLink *lp) {
  XrdRedisProtocol *rp = new XrdRedisProtocol();
  rp->Link = lp;
  return rp;
}

void XrdRedisProtocol::Reset() {

}

void XrdRedisProtocol::Recycle(XrdLink *lp,int consec,const char *reason) {

}

int XrdRedisProtocol::Stats(char *buff, int blen, int do_sync) {
  return 0;
}

void XrdRedisProtocol::DoIt() {

}

int XrdRedisProtocol::Configure(char *parms, XrdProtocol_Config * pi) {
  eDest.logger(pi->eDest->logger());

  char* rdf = (parms && *parms ? parms : pi->ConfigFN);
  bool success = Configuration::fromFile(rdf, configuration);
  if(success) return 1;
  return 0;
}

XrdRedisProtocol::~XrdRedisProtocol() {

}
