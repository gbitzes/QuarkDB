// ----------------------------------------------------------------------
// File: RaftParser.cc
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

#include "RaftParser.hh"
#include "../Utils.hh"
using namespace quarkdb;

bool RaftParser::appendEntries(RedisRequest &&source, RaftAppendEntriesRequest &dest) {
  //----------------------------------------------------------------------------
  // We assume source[0] is correct, ie "raft_append_entries"
  //----------------------------------------------------------------------------

  // 7 chunks is the minimum for a 0-entries request
  if(source.size() < 7) return false;

  if(!my_strtoll(source[1], dest.term)) return false;
  if(!parseServer(source[2], dest.leader)) return false;
  if(!my_strtoll(source[3], dest.prevIndex)) return false;
  if(!my_strtoll(source[4], dest.prevTerm)) return false;
  if(!my_strtoll(source[5], dest.commitIndex)) return false;

  int64_t nreqs;
  if(!my_strtoll(source[6], nreqs)) return false;
  if((int) source.size() < 7 + (nreqs*3)) return false;

  int64_t index = 7;
  for(int64_t i = 0; i < nreqs; i++) {
    int64_t reqsize;
    if(!my_strtoll(source[index], reqsize)) return false;
    if((int) source.size() < index+2+reqsize) return false;

    RaftEntry tmp;
    if(!my_strtoll(source[index+1], tmp.term)) return false;
    for(int64_t j = 0; j < reqsize; j++) {
      tmp.request.emplace_back(std::move(source[index+2+j]));
    }

    dest.entries.emplace_back(std::move(tmp));
    index += 2+reqsize;
  }

  if(index != (int64_t) source.size()) return false;
  return true;
}
