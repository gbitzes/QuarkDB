// ----------------------------------------------------------------------
// File: XrdRedisProtocol.hh
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

#ifndef __QUARKDB_XRDREDIS_PROTOCOL_H__
#define __QUARKDB_XRDREDIS_PROTOCOL_H__

#include "Xrd/XrdProtocol.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "Xrd/XrdLink.hh"
#include "Xrd/XrdBuffer.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucStream.hh"

#include "QuarkDBNode.hh"

#include "Configuration.hh"
#include "RedisParser.hh"
#include "RocksDB.hh"
#include "Dispatcher.hh"
#include "raft/RaftJournal.hh"
#include "raft/RaftState.hh"
#include "raft/RaftTimeouts.hh"
#include "raft/RaftDirector.hh"

namespace quarkdb {


class XrdRedisProtocol : public XrdProtocol {
public:
  /// Read and apply the configuration
  static int Configure(char *parms, XrdProtocol_Config *pi);

  /// Implementation of XrdProtocol interface
  XrdProtocol *Match(XrdLink *lp);
  int Process(XrdLink *lp);
  void Recycle(XrdLink *lp=0,int consec=0,const char *reason=0);
  int Stats(char *buff, int blen, int do_sync=0);

  /// Implementation of XrdJob interface
  void DoIt();

  /// Construction / destruction
  XrdRedisProtocol();
  virtual ~XrdRedisProtocol();

  /// globally accessible error handler
  static XrdSysError eDest;

  static std::atomic<bool> inShutdown;
  static EventFD shutdownFD;
private:
  /// The link we are bound to
  Link *link = nullptr;
  RedisParser *parser = nullptr;
  Connection *conn = nullptr;

  RedisRequest currentRequest;
  void Reset();
protected:
  static XrdBuffManager *bufferManager;
  static QuarkDBNode *quarkdbNode;

  static void shutdownMonitor();
  static std::atomic<int64_t> inFlight;
};

}


#endif
