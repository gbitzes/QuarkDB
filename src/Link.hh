// ----------------------------------------------------------------------
// File: Link.hh
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

#ifndef __QUARKDB_LINK_H__
#define __QUARKDB_LINK_H__

#include <sstream>
#include "Xrd/XrdLink.hh"
#include <qclient/QClient.hh>

namespace quarkdb {

//------------------------------------------------------------------------------
// Return code from XrdLink.
// 1 or higher means success. The value is typically the number of bytes read.
// 0 means slow link, ie there's not enough data yet to complete the operation.
//   This is not an error, and you should retry later.
// Negative means an error occured.
//------------------------------------------------------------------------------
using LinkStatus = int;

//------------------------------------------------------------------------------
// Our link class either directly maps to an XrdLink, or to an internal buffer.
// Needed for unit tests.
//------------------------------------------------------------------------------

class Link {
public:
  Link(const qclient::TlsConfig &tlsconfig_);

  Link(XrdLink *lp, qclient::TlsConfig tlsconfig = {} );
  Link() : Link(qclient::TlsConfig()) {}
  Link(int fd_, qclient::TlsConfig tlsconfig = {} );
  ~Link();

  LinkStatus Recv(char *buff, int blen, int timeout);
  LinkStatus Send(const char *buff, int blen);
  LinkStatus Close(int defer = 0);

  // not present in XrdLink, but convenient
  LinkStatus Send(const std::string &str);
  std::string describe() const;
  std::string getID() const { return uuid; }

  bool isLocalhost() const;
  void overrideHost(const std::string &host);

  // Set global connection logging config
  static void setConnectionLogging(bool val);
private:
  qclient::TlsConfig tlsconfig;
  qclient::TlsFilter tlsfilter;

  std::stringstream stream;
  XrdLink *link = nullptr;
  bool dead = false;
  int fd = -1;

  std::string uuid;
  std::string host;

  LinkStatus streamRecv(char *buff, int blen, int timeout);
  LinkStatus streamSend(const char *buff, int blen);
  LinkStatus streamClose(int defer = 0);

  LinkStatus fdRecv(char *buff, int blen, int timeout);
  LinkStatus fdSend(const char *buff, int blen);
  LinkStatus fdClose(int defer = 0);

  LinkStatus rawRecv(char *buff, int blen, int timeout);
  LinkStatus rawSend(const char *buff, int blen);

  qclient::RecvStatus recvStatus(char *buff, int blen, int timeout);
};

}
#endif
