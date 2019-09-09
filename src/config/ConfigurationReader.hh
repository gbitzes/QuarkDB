// ----------------------------------------------------------------------
// File: ConfigurationReader.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef QUARKDB_CONFIGURATION_READER_HH
#define QUARKDB_CONFIGURATION_READER_HH

#include <string>

namespace quarkdb {

//------------------------------------------------------------------------------
// Helper class to move through the contents of a configuration file
//------------------------------------------------------------------------------
class ConfigurationReader {
public:
  //----------------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------------
  ConfigurationReader(const std::string &str);

  //----------------------------------------------------------------------------
  // Get current word
  //----------------------------------------------------------------------------
  std::string getCurrentWord() const;

  //----------------------------------------------------------------------------
  // Advance to next word
  //----------------------------------------------------------------------------
  void advanceWord();

  //----------------------------------------------------------------------------
  // Advance to next line
  //----------------------------------------------------------------------------
  void advanceLine();

  //----------------------------------------------------------------------------
  // Reached EOF?
  //----------------------------------------------------------------------------
  bool eof() const;

private:
  //----------------------------------------------------------------------------
  // Find next whitespace
  //----------------------------------------------------------------------------
  size_t findNextWhitespace() const;

  //----------------------------------------------------------------------------
  // Find next non-whitespace
  //----------------------------------------------------------------------------
  size_t findNextNonWhitespace() const;



  std::string mContents;
  size_t mPosition = 0;
};

}

#endif
