// ----------------------------------------------------------------------
// File: response-formatter.cc
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

#include "Formatter.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(Response, T1) {
  ASSERT_EQ(Formatter::err("ERR test"), "-ERR test\r\n");
  ASSERT_EQ(Formatter::ok(), "+OK\r\n");
  ASSERT_EQ(Formatter::pong(), "+PONG\r\n");
  ASSERT_EQ(Formatter::null(), "$-1\r\n");
  ASSERT_EQ(Formatter::status("test"), "+test\r\n");
}
