// ----------------------------------------------------------------------
// File: Link.cc
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

#include <iostream>
#include <limits>

#include "Link.hh"
#include "Common.hh"
#include "Utils.hh"

using namespace quarkdb;

int Link::Recv(char *buff, int blen, int timeout) {
  if(link) return link->Recv(buff, blen, timeout);
  return streamRecv(buff, blen, timeout);
}

int Link::Close(int defer) {
  if(link) return link->Close(defer);
  return streamClose(defer);
}

int Link::Send(const char *buff, int blen) {
  if(link) return link->Send(buff, blen);
  return streamSend(buff, blen);
}

int Link::Send(const std::string &str) {
  return Send(str.c_str(), str.size());
}

int Link::streamSend(const char *buff, int blen) {
  if(stream.eof()) return -1;
  stream.write(buff, blen);
  return blen;
}

int Link::streamClose(int defer) {
  stream.ignore(std::numeric_limits<std::streamsize>::max());
  return 0;
}

int Link::streamRecv(char *buff, int blen, int timeout) {
  if(stream.eof()) return -1;

  int totalRead = 0;
  while(true) {
    int rc = stream.readsome(buff, blen);
    totalRead += rc;

    blen -= rc;
    buff += rc;

    if(rc == 0 || blen == 0) break;
  }

  return totalRead;
}
