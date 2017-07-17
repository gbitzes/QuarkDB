// ----------------------------------------------------------------------
// File: FileDescriptor.hh
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

#ifndef __QUARKDB_FILE_DESCRIPTOR_H__
#define __QUARKDB_FILE_DESCRIPTOR_H__

#include <unistd.h>

namespace quarkdb {

class FileDescriptor {
public:
  FileDescriptor(int fd_) : fd(fd_) {
    if(fd < 0) {
      // We assume that EventDescriptor immediatelly wraps a call which
      // returns a file descriptor, so errno still contains the error we're
      // interested in.
      localerrno = errno;
    }
  }

  FileDescriptor() {}

  ~FileDescriptor() {
    close();
  }

  void close() {
    if(fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  bool ok() {
    return fd >= 0 && localerrno == 0;
  }

  std::string err() {
    return strerror(localerrno);
  }

  int getFD() {
    return fd;
  }

private:
  int localerrno = 0;
  int fd = -1;

};

}

#endif
