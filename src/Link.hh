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
// Our link class either directly maps to an XrdLink, or to an internal buffer.
// Needed for unit tests.
//------------------------------------------------------------------------------

class Link {
public:
  Link(XrdLink *lp) : link(lp) {}
  Link() {}

  int Recv(char *buff, int blen, int timeout);
  int Send(const char *buff, int blen);
  int Close(int defer = 0);

  // not present in XrdLink, but convenient
  int Send(const std::string &str);
private:
  std::stringstream stream;
  XrdLink *link = nullptr;

  int streamRecv(char *buff, int blen, int timeout);
  int streamSend(const char *buff, int blen);
  int streamClose(int defer = 0);
};

}
#endif
