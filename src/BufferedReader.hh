// ----------------------------------------------------------------------
// File: BufferedReader.hh
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

#ifndef __QUARKDB_BUFFERED_READER_H__
#define __QUARKDB_BUFFERED_READER_H__

#include <deque>
#include <string>

#include "Link.hh"

#include "Xrd/XrdLink.hh"
#include "Xrd/XrdBuffer.hh"

namespace quarkdb {

//------------------------------------------------------------------------------
// Return code from XrdLink.
// 1 or higher means success. The value is typically the number of bytes read.
// 0 means slow link, ie there's not enough data yet to complete the operation.
//   This is not an error, and you should retry later.
// Negative means an error occured.
//------------------------------------------------------------------------------
using LinkStatus = int;


class BufferedReader {
public:
  BufferedReader(Link *lp, XrdBuffManager *bpool, size_t bsize = 1024 * 32);

  //----------------------------------------------------------------------------
  // Read exactly len bytes from the link. An all-or-nothing operation -
  // either it succeeds and we get len bytes, or there's not enough data on the
  // link yet and we get nothing.
  //----------------------------------------------------------------------------
  LinkStatus consume(size_t len, std::string &str);
private:
  Link *link;

  //----------------------------------------------------------------------------
  // Buffer manager that recycles memory buffers
  //----------------------------------------------------------------------------
  XrdBuffManager *bufferPool;

  //----------------------------------------------------------------------------
  // We use a deque of buffers for reading from the socket.
  // We always append new buffers to this deque - once a buffer is full, we
  // allocate a new one. Once the contents of a buffer have been parsed, we
  // release it.
  //
  // The buffer manager will take care of recycling the memory and prevent
  // unnecessary alloactions.
  //----------------------------------------------------------------------------

  std::deque<XrdBuffer*> buffers;
  size_t position_read; // always points to the buffer at the front
  size_t position_write; // always points to the buffer at the end
  const size_t buffer_size;


  //----------------------------------------------------------------------------
  // Read from the link as much data as is currently available
  //----------------------------------------------------------------------------
  LinkStatus readFromLink();

  //----------------------------------------------------------------------------
  // Is it possible to consume len bytes?
  // Returns 0 if not, negative on error, or the number of bytes that is
  // possible to read if and only if that amount is greater than len
  //----------------------------------------------------------------------------
  LinkStatus canConsume(size_t len);
};

}

#endif
