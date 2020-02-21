// ----------------------------------------------------------------------
// File: ParanoidManifestChecker.cc
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * quarkdb - a redis-like highly available key-value store              *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "storage/ParanoidManifestChecker.hh"
#include "utils/DirectoryIterator.hh"
#include "utils/StringUtils.hh"
#include <sys/stat.h>

namespace quarkdb {

ParanoidManifestChecker::ParanoidManifestChecker(std::string_view path)
: mPath(path) {
  mThread.reset(&ParanoidManifestChecker::main, this);
}

void ParanoidManifestChecker::main(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {

    Status st = checkDB(mPath);
    if(!st.ok()) {
      qdb_error("Potential MANIFEST corruption for DB at " << mPath << "(" << st.getMsg() << ")");
    }

    mLastStatus.set(st);
    assistant.wait_for(std::chrono::minutes(5));
  }
}

bool operator<(struct timespec &one, struct timespec &two) {
  if(one.tv_sec == two.tv_sec) {
    return one.tv_nsec < two.tv_nsec;
  }

  return one.tv_sec < two.tv_sec;
}

Status ParanoidManifestChecker::checkDB(std::string_view path) {
  DirectoryIterator iter(path);
  struct dirent* entry = nullptr;

  struct timespec manifestMtime;
  struct timespec sstMtime;

  while((entry = iter.next())) {
    struct stat statbuf;

    if(stat(SSTR(path << "/" << entry->d_name).c_str(), &statbuf) == 0) {
      if(StringUtils::startsWith(entry->d_name, "MANIFEST") && manifestMtime < statbuf.st_mtim) {
        manifestMtime = statbuf.st_mtim;
      }

      if(StringUtils::endsWith(entry->d_name, ".sst") && sstMtime < statbuf.st_mtim) {
        sstMtime = statbuf.st_mtim;
      }
    }
  }

  int secDiff = sstMtime.tv_sec - manifestMtime.tv_sec;
  std::string diff = SSTR(secDiff << " sec");

  // 1 hour should be more than enough (?)
  if(secDiff >= 3600) {
    return Status(1, diff);
  }

  return Status(0, diff);
}

//------------------------------------------------------------------------------
// Get last status
//------------------------------------------------------------------------------
Status ParanoidManifestChecker::getLastStatus() const {
  return mLastStatus.get();
}

}
