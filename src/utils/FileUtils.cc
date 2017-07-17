// ----------------------------------------------------------------------
// File: FileUtils.cc
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

#include "FileUtils.hh"
#include "../Utils.hh"
#include <sys/stat.h>
#include <string.h>

namespace quarkdb {

std::string pathJoin(const std::string &part1, const std::string &part2) {
  if(part1.empty()) return "/" + part2;
  if(part2.empty()) return part1;
  if(part1[part1.size()-1] == '/') return part1 + part2;
  return part1 + "/" + part2;
}

std::string pathJoin(const std::string &part1, const std::string &part2, const std::string &part3) {
  return pathJoin(part1, pathJoin(part2, part3));
}

std::string chopPath(const std::string &path) {
  std::vector<std::string> parts = split(path, "/");
  std::stringstream ss;

  for(size_t i = 1; i < parts.size()-1; i++) {
    ss << "/" << parts[i];
  }

  return ss.str();
}

bool mkpath(const std::string &path, mode_t mode, std::string &err) {
  size_t pos = path.find("/");

  while( (pos = path.find("/", pos+1)) != std::string::npos) {
    std::string chunk = path.substr(0, pos);

    struct stat sb;
    if(stat(chunk.c_str(), &sb) != 0) {
      qdb_info("Creating directory: " << chunk);
      if(mkdir(chunk.c_str(), mode) < 0) {
        int localerrno = errno;
        err = SSTR("cannot create directory " << chunk << ": " << strerror(localerrno));
        return false;
      }
    }
  }

  return true;
}

void mkpath_or_die(const std::string &path, mode_t mode) {
  std::string err;
  if(!quarkdb::mkpath(path, mode, err)) qdb_throw(err);
}

bool directoryExists(const std::string &path, std::string &err) {
  struct stat sb;

  if(stat(path.c_str(), &sb) != 0) {
    err = SSTR("Cannot stat " << path);
    return false;
  }

  if(!S_ISDIR(sb.st_mode)) {
    err = SSTR(path << " is not a directory");
    return false;
  }

  return true;
}

bool readFile(FILE *f, std::string &contents) {
  bool retvalue = true;
  std::ostringstream ss;

  const int BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];

  while(true) {
    size_t bytesRead = fread(buffer, 1, BUFFER_SIZE, f);

    if(bytesRead > 0) {
      ss.write(buffer, bytesRead);
    }

    // end of file
    if(bytesRead != BUFFER_SIZE) {
      retvalue = feof(f);
      break;
    }
  }

  contents = ss.str();
  return retvalue;
}

bool readFile(const std::string &path, std::string &contents) {
  bool retvalue = true;

  FILE *in = fopen(path.c_str(), "rb");
  if(!in) {
    return false;
  }

  retvalue = readFile(in, contents);
  fclose(in);
  return retvalue;
}

bool write_file(const std::string &path, const std::string &contents, std::string &err) {
  bool retvalue;

  FILE *out = fopen(path.c_str(), "wb");

  if(!out) {
    err = SSTR("Unable to open path for writing: " << path << ", errno: " << errno);
    return false;
  }

  retvalue = fwrite(contents.c_str(), sizeof(char), contents.size(), out);
  fclose(out);
  return retvalue;
}

void write_file_or_die(const std::string &path, const std::string &contents) {
  std::string err;
  if(!write_file(path, contents, err)) {
    qdb_throw(err);
  }
}

void rename_directory_or_die(const std::string &source, const std::string &destination) {
  qdb_info("Renaming directory: '" << source << "' to '" << destination << "'");

  std::string tmp;
  if(!directoryExists(source, tmp)) {
    qdb_throw("Tried to rename " << q(source) << " to " << q(destination) << ", but " << q(source) << " did not exist.");
  }

  int ret = rename(source.c_str(), destination.c_str());
  if(ret != 0) {
    qdb_throw("Tried to rename " << q(source) << " to " << q(destination) << ", but ::rename failed: " << strerror(errno));
  }
}


}
