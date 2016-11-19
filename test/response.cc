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

#include "Response.hh"
#include "BufferedReader.hh"
#include <gtest/gtest.h>

using namespace quarkdb;

TEST(Response, T1) {
  Link link;
  std::string buffer;
  XrdBuffManager bufferManager(NULL, NULL);
  BufferedReader reader(&link, &bufferManager, 3);

  ASSERT_EQ(Response::err(&link, "test"), 11);
  ASSERT_EQ(reader.consume(11, buffer), 11);
  ASSERT_EQ(buffer, "-ERR test\r\n");

  ASSERT_EQ(Response::ok(&link), 5);
  ASSERT_EQ(reader.consume(5, buffer), 5);
  ASSERT_EQ(buffer, "+OK\r\n");

  ASSERT_EQ(Response::pong(&link), 7);
  ASSERT_EQ(reader.consume(7, buffer), 7);
  ASSERT_EQ(buffer, "+PONG\r\n");

  ASSERT_EQ(Response::null(&link), 5);
  ASSERT_EQ(reader.consume(5, buffer), 5);
  ASSERT_EQ(buffer, "$-1\r\n");

  ASSERT_EQ(Response::status(&link, "test"), 7);
  ASSERT_EQ(reader.consume(7, buffer), 7);
  ASSERT_EQ(buffer, "+test\r\n");
}
