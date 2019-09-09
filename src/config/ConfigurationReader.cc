// ----------------------------------------------------------------------
// File: ConfigurationReader.cc
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

#include "config/ConfigurationReader.hh"
#include "utils/Macros.hh"
#include <sstream>

namespace quarkdb {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ConfigurationReader::ConfigurationReader(const std::string &str)
: mContents(str), mPosition(0) {

  if(!mContents.empty() && isspace(mContents[mPosition])) {
    advanceWord();
  }
}

//------------------------------------------------------------------------------
// Get current word
//------------------------------------------------------------------------------
std::string ConfigurationReader::getCurrentWord() const {
  if(mPosition >= mContents.size()) {
    return "";
  }

  std::ostringstream ss;
  size_t pos = mPosition;

  while(pos < mContents.size() && !isspace(mContents[pos])) {
    ss << mContents[pos];
    pos++;
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Advance to next word
//------------------------------------------------------------------------------
void ConfigurationReader::advanceWord() {
  mPosition = findNextWhitespace();
  mPosition = findNextNonWhitespace();
}

//------------------------------------------------------------------------------
// Advance to next line
//------------------------------------------------------------------------------
void ConfigurationReader::advanceLine() {
  while(mPosition < mContents.size()) {
    mPosition++;
    if(mContents[mPosition] == '\n') {
      mPosition++;
      break;
    }
  }
}

//------------------------------------------------------------------------------
// Reached EOF?
//------------------------------------------------------------------------------
bool ConfigurationReader::eof() const {
  return mPosition >= mContents.size();
}

//------------------------------------------------------------------------------
// Find next whitespace
//------------------------------------------------------------------------------
size_t ConfigurationReader::findNextWhitespace() const {
  size_t pos = mPosition;

  while(pos < mContents.size() && !isspace(mContents[pos])) {
    pos++;
  }

  return pos;
}

//------------------------------------------------------------------------------
// Find next non-whitespace
//------------------------------------------------------------------------------
size_t ConfigurationReader::findNextNonWhitespace() const {
  size_t pos = mPosition;
  while(pos < mContents.size() && isspace(mContents[pos])) {
    pos++;
  }

  return pos;
}


}
