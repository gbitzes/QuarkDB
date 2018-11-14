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

  Authenticator(std::string_view secret);
  std::string generateChallenge(std::string_view opponentRandomBytes, std::chrono::system_clock::time_point timestamp, std::string_view myRandomBytes);
  std::string generateChallenge(std::string_view opponentRandomBytes);
  ValidationStatus validateSignature(std::string_view signature);
  void resetDeadline();
  ~Authenticator() {}

  static std::string generateSignature(std::string_view stringToSign, std::string_view key);
  ValidationStatus validateSignatureNoDeadline(std::string_view stringToSign);

private:
  std::string_view secretKey;

  std::string challengeString;
  std::chrono::steady_clock::time_point challengeDeadline;
};

}

#endif
