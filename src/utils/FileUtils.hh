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

std::string pathJoin(std::string_view part1, std::string_view part2);
bool mkpath(const std::string &path, mode_t mode, std::string &err);
void mkpath_or_die(const std::string &path, mode_t mode);
bool directoryExists(const std::string &path, std::string &err);
bool fileExists(const std::string &path, std::string &err);
bool readFile(FILE *f, std::string &contents);
bool readFile(const std::string &path, std::string &contents);
bool write_file(std::string_view path, std::string_view contents, std::string &err);
void write_file_or_die(std::string_view path, std::string_view contents);
void rename_directory_or_die(const std::string &source, const std::string &destination);
bool areFilePermissionsSecure(mode_t mode);
bool readPasswordFile(const std::string &path, std::string &contents);

}

#endif
