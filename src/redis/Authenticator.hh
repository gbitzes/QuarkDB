// ----------------------------------------------------------------------
// File: Authenticator.hh
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

#ifndef QUARKDB_AUTHENTICATOR_H
#define QUARKDB_AUTHENTICATOR_H

#include <string>
#include <chrono>

namespace quarkdb {


class Authenticator {
public:
  enum class ValidationStatus {
    kOk,
    kDeadlinePassed,
    kInvalidSignature
  };

  Authenticator(const std::string &secret);
  std::string generateChallenge(const std::string &opponentRandomBytes, const std::chrono::system_clock::time_point &timestamp, const std::string &myRandomBytes);
  std::string generateChallenge(const std::string &opponentRandomBytes);
  ValidationStatus validateSignature(const std::string &signature);
  void resetDeadline();
  ~Authenticator() {}

  static std::string generateSignature(const std::string &stringToSign, const std::string &key);
  ValidationStatus validateSignatureNoDeadline(const std::string &stringToSign);

private:
  const std::string &secretKey;

  std::string challengeString;
  std::chrono::steady_clock::time_point challengeDeadline;
};

}

#endif