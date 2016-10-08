// ----------------------------------------------------------------------
// File: EventFD.hh
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

#ifndef __QUARKDB_EVENTFD_H__
#define __QUARKDB_EVENTFD_H__

#include <sys/eventfd.h>
#include <unistd.h>

namespace quarkdb {

class EventFD {
public:
  EventFD() {
    fd = eventfd(0, EFD_NONBLOCK);
    reset();
  }

  ~EventFD() {
    if(fd >= 0) {
      close(fd);
    }
  }

  void notify(int64_t val = 1) {
    write(fd, &val, sizeof(val));
  }

  int64_t reset() {
    int tmp;
    ::read(fd, &tmp, sizeof(tmp));
    return tmp;
  }

  int getFD() {
    return fd;
  }
private:
  int fd = -1;
};

}

#endif
