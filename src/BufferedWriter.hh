//----------------------------------------------------------------------
// File: BufferedWriter.hh
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

#ifndef __QUARKDB_BUFFERED_WRITER_H__
#define __QUARKDB_BUFFERED_WRITER_H__

#include <mutex>

namespace quarkdb {
using LinkStatus = int;
class Link;

#define OUTPUT_BUFFER_SIZE (16*1024)

class BufferedWriter {
public:
  BufferedWriter(Link *link);
  ~BufferedWriter();

  void setActive(bool newval);
  void flush();
  LinkStatus send(std::string &&raw);

  class FlushGuard {
  public:
    FlushGuard(BufferedWriter *w) : writer(w) { }
    ~FlushGuard() { writer->flush(); }
  private:
    BufferedWriter *writer;
  };
private:
  Link *link;

  bool active = true;
  char buffer[OUTPUT_BUFFER_SIZE];
  int bufferedBytes = 0;

  std::recursive_mutex mtx;
};

}

#endif
