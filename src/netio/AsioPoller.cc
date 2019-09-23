// ----------------------------------------------------------------------
// File: AsioPoller.cc
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

#include "netio/AsioPoller.hh"
#include "Link.hh"
#include "Connection.hh"
#include <qclient/TlsFilter.hh>

namespace quarkdb {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
AsioPoller::AsioPoller(int port, size_t threadPoolSize, Dispatcher *disp)
: mPort(port), mThreadPoolSize(threadPoolSize), mDispatcher(disp),
  mAcceptor(mContext, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), mPort)),
  mNextSocket(mContext)
 {

  asio::socket_base::reuse_address option(true);
  mAcceptor.set_option(option);

  requestAccept();

  for(size_t i = 0; i < mThreadPoolSize; i++) {
    mThreadPool.emplace_back(&AsioPoller::workerThread, this);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
AsioPoller::~AsioPoller() {
  mShutdown = true;
  mAcceptor.close();
  mContext.stop();

  for(size_t i = 0; i < mThreadPool.size(); i++) {
    mThreadPool[i].join();
  }

  mEntries.clear();
}

//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------
void AsioPoller::workerThread(ThreadAssistant &assistant) {
  mContext.run();
}

//------------------------------------------------------------------------------
// Request next async accept
//------------------------------------------------------------------------------
void AsioPoller::requestAccept() {
  mNextSocket = asio::ip::tcp::socket(mContext);
  mAcceptor.async_accept(
    mNextSocket,
    std::bind(&AsioPoller::handleAccept, this, std::placeholders::_1)
  );
}

//------------------------------------------------------------------------------
// Handle incoming TCP connect
//------------------------------------------------------------------------------
void AsioPoller::handleAccept(const std::error_code& ec) {
  if(!ec) {
    mNextSocket.non_blocking(true);

    std::unique_ptr<ActiveEntry> activeEntry;
    activeEntry.reset(new ActiveEntry(std::move(mNextSocket)));

    qclient::TlsConfig tlsconfig;
    activeEntry->link = new Link(activeEntry->socket, tlsconfig);
    activeEntry->conn = new Connection(activeEntry->link);

    ActiveEntry *ptr = activeEntry.get();
    mEntries[ptr] = std::move(activeEntry);

    ptr->socket.async_wait(asio::ip::tcp::socket::wait_read,
      std::bind(&AsioPoller::handleWait, this, ptr, std::placeholders::_1));
  }

  if(!mShutdown) {
    requestAccept();
  }
}

//------------------------------------------------------------------------------
// ActiveEntry destructor
//------------------------------------------------------------------------------
ActiveEntry::~ActiveEntry() {
  delete conn;
  delete link;
}

//------------------------------------------------------------------------------
// Handle wait
//------------------------------------------------------------------------------
void AsioPoller::handleWait(ActiveEntry *entry, const std::error_code& ec) {
  LinkStatus status = entry->conn->processRequests(mDispatcher, mInFlightTracker);
  if(ec.value() == 0 && status >= 0) {
    entry->socket.async_wait(asio::ip::tcp::socket::wait_read,
      std::bind(&AsioPoller::handleWait, this, entry, std::placeholders::_1));
  }
  else {
    mEntries.erase(entry);
  }
}

}
