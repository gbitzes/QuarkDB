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
  Link(XrdLink *lp) : link(lp) {}
  Link() {}
  Link(int fd_);

  LinkStatus Recv(char *buff, int blen, int timeout);
  LinkStatus Send(const char *buff, int blen);
  LinkStatus Close(int defer = 0);

  // not present in XrdLink, but convenient
  LinkStatus Send(const std::string &str);
private:
  std::stringstream stream;
  XrdLink *link = nullptr;
  int fd = -1;

  LinkStatus streamRecv(char *buff, int blen, int timeout);
  LinkStatus streamSend(const char *buff, int blen);
  LinkStatus streamClose(int defer = 0);

  LinkStatus fdRecv(char *buff, int blen, int timeout);
  LinkStatus fdSend(const char *buff, int blen);
  LinkStatus fdClose(int defer = 0);
};

}
#endif
