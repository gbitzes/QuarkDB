// ----------------------------------------------------------------------
// File: Authenticator.cc
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

#include <sstream>
#include "Authenticator.hh"
#include "../utils/Macros.hh"
#include "../utils/Random.hh"
#include "../utils/StringUtils.hh"

#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace quarkdb {

Authenticator::Authenticator(const std::string &secret) : secretKey(secret) {
  if(!secret.empty() && secret.size() < 32) {
    qdb_throw("Secret key is too small! Minimum size: 32");
  }
}

std::string Authenticator::generateChallenge(const std::string &opponentRandomBytes, const std::chrono::system_clock::time_point &timestamp, const std::string &myRandomBytes) {
  qdb_assert(opponentRandomBytes != myRandomBytes);

  // Calculate the deadline - responses will not be accepted after this much time
  // has elapsed.
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  challengeDeadline = now + std::chrono::minutes(1);

  // Convert the timepoint to string
  std::string strTimePoint = std::to_string(
    std::chrono::duration_cast<std::chrono::milliseconds>(timestamp.time_since_epoch()).count()
  );

  qdb_assert(opponentRandomBytes.size() == 64);
  qdb_assert(myRandomBytes.size() == 64);
  challengeString = SSTR(opponentRandomBytes << "---" << strTimePoint << "---" << myRandomBytes);
  return challengeString;
}

std::string Authenticator::generateChallenge(const std::string &opponentRandomBytes) {
  qdb_assert(opponentRandomBytes.size() == 64);

  // Calculate a timepoint based on system_clock to make the challenge more difficult.
  // We don't use steady_clock, as that leaks information (machine uptime) to
  // unauthorized users. (not really important, but let's be paranoid)

  return generateChallenge(
    opponentRandomBytes,
    std::chrono::system_clock::now(),
    generateSecureRandomBytes(64)
  );
}

std::string Authenticator::generateSignature(const std::string &stringToSign, const std::string &key) {
  std::string ret;
  ret.resize(SHA256_DIGEST_LENGTH);

  unsigned int bufferLen = SHA256_DIGEST_LENGTH;

  HMAC(EVP_sha256(), (const unsigned char*) key.c_str(), key.size(),
    (const unsigned char*) stringToSign.c_str(), stringToSign.size(), (unsigned char*) ret.data(), &bufferLen);

  return ret;
}

Authenticator::ValidationStatus Authenticator::validateSignature(const std::string &signature) {
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
  auto deadline = challengeDeadline;
  resetDeadline();

  if(deadline < now) {
    return ValidationStatus::kDeadlinePassed;
  }

  if(signature != Authenticator::generateSignature(challengeString, secretKey)) {
    return ValidationStatus::kInvalidSignature;
  }

  return ValidationStatus::kOk;
}

void Authenticator::resetDeadline() {
  challengeDeadline = {};
}



}
