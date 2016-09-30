// ----------------------------------------------------------------------
// File: buffered-reader.cc
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

#include "BufferedReader.hh"
#include <gtest/gtest.h>

using namespace quarkdb;


class Buffered_Reader : public ::testing::TestWithParam<int> {
protected:
  virtual void SetUp() {
    bufferManager = new XrdBuffManager(NULL, NULL);
    reader = new BufferedReader(&link, bufferManager, GetParam());
  }

  virtual void TearDown() {
    delete reader;
    delete bufferManager;
  }

  Link link;
  XrdBuffManager *bufferManager;
  BufferedReader *reader;
  std::string buffer;
};

INSTANTIATE_TEST_CASE_P(TryVariousBufferSizes,
                        Buffered_Reader,
                        ::testing::Values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 100, 200));

TEST_P(Buffered_Reader, T1) {
  ASSERT_EQ(link.Send("adfadfaF"), 8);

  ASSERT_EQ(reader->consume(5, buffer), 5);
  ASSERT_EQ(buffer, "adfad");

  ASSERT_EQ(reader->consume(4, buffer), 0);
  ASSERT_EQ(reader->consume(3, buffer), 3);
  ASSERT_EQ(buffer, "faF");

  ASSERT_EQ(reader->consume(1, buffer), 0);

  ASSERT_EQ(link.Send("1234567890"), 10);
  ASSERT_EQ(reader->consume(11, buffer), 0);
  ASSERT_EQ(reader->consume(100, buffer), 0);
  ASSERT_EQ(reader->consume(3, buffer), 3);
  ASSERT_EQ(buffer, "123");

  ASSERT_EQ(reader->consume(8, buffer), 0);
  ASSERT_EQ(link.Send("123"), 3);

  ASSERT_EQ(reader->consume(10, buffer), 10);
  ASSERT_EQ(buffer, "4567890123");

  ASSERT_EQ(link.Send(std::string(2048, 'q')), 2048);

  ASSERT_EQ(reader->consume(10, buffer), 10);
  ASSERT_EQ(buffer, std::string(10, 'q'));

  ASSERT_EQ(reader->consume(2039, buffer), 0);
  ASSERT_EQ(reader->consume(2038, buffer), 2038);
  ASSERT_EQ(buffer, std::string(2038, 'q'));

  ASSERT_EQ(reader->consume(1, buffer), 0);
  ASSERT_EQ(link.Close(), 0);
  ASSERT_LT(reader->consume(1, buffer), 0);
}
