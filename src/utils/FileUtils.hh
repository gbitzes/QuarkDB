// ----------------------------------------------------------------------
// File: FileUtils.hh
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

#ifndef __QUARKDB_FILE_UTILS_H__
#define __QUARKDB_FILE_UTILS_H__

#include <string>

namespace quarkdb {

std::string pathJoin(const std::string &part1, const std::string &part2);
std::string chopPath(const std::string &path);
bool mkpath(const std::string &path, mode_t mode, std::string &err);
void mkpath_or_die(const std::string &path, mode_t mode);
bool directoryExists(const std::string &path, std::string &err);
bool readFile(FILE *f, std::string &contents);
bool readFile(const std::string &path, std::string &contents);
bool write_file(const std::string &path, const std::string &contents);
void write_file_or_die(const std::string &path, const std::string &contents);


}

#endif
