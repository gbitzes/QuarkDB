// ----------------------------------------------------------------------
// File: AsioPoller.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QUARKDB_ASIO_POLLER_HH
#define QUARKDB_ASIO_POLLER_HH

#include "utils/AssistedThread.hh"
#include "utils/InFlightTracker.hh"
#include "EventFD.hh"
#include <asio.hpp>
#include <map>

namespace quarkdb {

class Dispatcher;
class Link;
class Connection;

struct ActiveEntry {
  ActiveEntry(asio::ip::tcp::socket &&sock) : socket(std::move(sock)) {}

  asio::ip::tcp::socket socket;
  Link *link;
  Connection *conn;

  ~ActiveEntry();
};

//------------------------------------------------------------------------------
// Listens at a specific network port, and handles redis connections using
// the given dispatcher.
//------------------------------------------------------------------------------
class AsioPoller {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  AsioPoller(int port, size_t threadPoolSize, Dispatcher *disp);

  //----------------------------------------------------------------------------
  // Destructor
  //----------------------------------------------------------------------------
  ~AsioPoller();

  //----------------------------------------------------------------------------
  // Thread pool
  //----------------------------------------------------------------------------
  void workerThread(ThreadAssistant &assistant);

private:
  //----------------------------------------------------------------------------
  // Request next async accept
  //----------------------------------------------------------------------------
  void requestAccept4();
  void requestAccept6();

  //----------------------------------------------------------------------------
  // Handle incoming TCP connect
  //----------------------------------------------------------------------------
  void handleAccept(asio::ip::tcp::socket socket);
  void handleAccept4(const std::error_code& ec);
  void handleAccept6(const std::error_code& ec);

  //----------------------------------------------------------------------------
  // Handle wait
  //----------------------------------------------------------------------------
  void handleWait(ActiveEntry *entry, const std::error_code& ec);


  std::atomic<bool> mShutdown = false;

  int mPort;
  size_t mThreadPoolSize;
  Dispatcher* mDispatcher;
  AssistedThread mMainThread;
  std::vector<AssistedThread> mThreadPool;

  InFlightTracker mInFlightTracker;

  asio::io_context mContext;
  asio::ip::tcp::acceptor mAcceptor4;
  asio::ip::tcp::acceptor mAcceptor6;

  asio::ip::tcp::socket mNextSocket4;
  asio::ip::tcp::socket mNextSocket6;

  std::mutex mEntriesMtx;
  std::map<ActiveEntry*, std::unique_ptr<ActiveEntry>> mEntries;
};

}

#endif
