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

#include "utils/FileUtils.hh"
#include "Utils.hh"
#include <sys/stat.h>
#include <string.h>

namespace quarkdb {

std::string pathJoin(std::string_view part1, std::string_view part2) {
  if(part1.empty()) return SSTR("/" << part2);
  if(part2.empty()) return SSTR(part1);
  if(part1[part1.size()-1] == '/') return SSTR(part1 << part2);
  return SSTR(part1 << "/" << part2);
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

bool readPasswordFile(const std::string &path, std::string &contents) {
  bool retvalue = true;

  FILE *in = fopen(path.c_str(), "rb");
  if(!in) {
    qdb_warn("Could not open " << path);
    return false;
  }

  // Ensure file permissions are 400.
  struct stat sb;
  if(fstat(fileno(in), &sb) != 0) {
    fclose(in);
    qdb_warn("Could not fstat " << path << " after opening (should never happen?!)");
    return false;
  }

  if(!areFilePermissionsSecure(sb.st_mode)) {
    qdb_warn("Refusing to read " << path << ", bad file permissions, should be 0400.");
    fclose(in);
    return false;
  }

  retvalue = readFile(in, contents);
  fclose(in);

  if(retvalue) {
    // Right trim any newlines and whitespace. By far the most common case will
    // be to have a single line in the password file. Users will expect to be
    // able to copy/paste that, let's not complicate matters with newlines.
    contents.erase(contents.find_last_not_of(" \t\n\r\f\v") + 1);
  }

  return retvalue;
}


bool areFilePermissionsSecure(mode_t mode) {
  if ((mode & 0077) != 0) {
    // Should disallow access to other users/groups
    return false;
  }

  if ((mode & 0700) != 0400) {
    // Just read access for user
    return false;
  }

  return true;
}

bool write_file(std::string_view path, std::string_view contents, std::string &err) {
  bool retvalue;

  FILE *out = fopen(std::string(path).c_str(), "wb");

  if(!out) {
    err = SSTR("Unable to open path for writing: " << path << ", errno: " << errno);
    return false;
  }

  retvalue = fwrite(contents.data(), sizeof(char), contents.size(), out);
  fclose(out);
  return retvalue;
}

void write_file_or_die(std::string_view path, std::string_view contents) {
  std::string err;
  if(!write_file(path, contents, err)) {
    qdb_throw(err);
  }
}

void rename_directory_or_die(const std::string &source, const std::string &destination) {
  qdb_info("Renaming directory: '" << source << "' to '" << destination << "'");

  std::string tmp;
  if(!directoryExists(source, tmp)) {
    qdb_throw("Tried to rename '" << source << "' to '" << destination << "', but '" << source << "' did not exist.");
  }

  int ret = rename(source.c_str(), destination.c_str());
  if(ret != 0) {
    qdb_throw("Tried to rename '" << source << "' to '" << destination << "', but ::rename failed: " << strerror(errno));
  }
}


}
