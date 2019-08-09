// ----------------------------------------------------------------------
// File: auth.cc
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
#include "utils/Macros.hh"
#include "utils/Random.hh"
#include "auth/AuthenticationDispatcher.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>
#include "qclient/QClient.hh"

using namespace qclient;
using namespace quarkdb;

TEST(FilePermissionChecking, BasicSanity) {
  ASSERT_FALSE(areFilePermissionsSecure(0700));
  ASSERT_FALSE(areFilePermissionsSecure(0777));
  ASSERT_FALSE(areFilePermissionsSecure(0477));
  ASSERT_FALSE(areFilePermissionsSecure(0401));
  ASSERT_FALSE(areFilePermissionsSecure(0455));
  ASSERT_FALSE(areFilePermissionsSecure(0444));
  ASSERT_FALSE(areFilePermissionsSecure(0404));
  ASSERT_FALSE(areFilePermissionsSecure(0440));
  ASSERT_FALSE(areFilePermissionsSecure(0500));
  ASSERT_FALSE(areFilePermissionsSecure(0700));

  ASSERT_TRUE(areFilePermissionsSecure(0400));
}

TEST(ReadPasswordFile, BasicSanity) {
  ASSERT_EQ(system("mkdir -p /tmp/quarkdb-tests/auth/"), 0);
  ASSERT_EQ(system("rm -f /tmp/quarkdb-tests/auth/f1"), 0);

  std::string contents;
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/non-existing", contents));
  ASSERT_FALSE(readFile("/tmp/quarkdb-tests/auth/non-existing", contents));

  ASSERT_EQ(system("echo 'pickles\n\n   ' > /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_EQ(system("chmod 777 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 744 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 700 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 500 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_FALSE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(system("chmod 400 /tmp/quarkdb-tests/auth/f1"), 0);
  ASSERT_TRUE(readPasswordFile("/tmp/quarkdb-tests/auth/f1", contents));
  ASSERT_EQ(contents, "pickles");

  ASSERT_EQ(system("rm -f /tmp/quarkdb-tests/auth/f1"), 0);
}

TEST(AuthenticationDispatcher, NoPassword) {
  AuthenticationDispatcher dispatcher("");
  std::unique_ptr<Authenticator> unused;

  bool authorized = false;
  ASSERT_EQ(Formatter::errArgs("AUTH"), dispatcher.dispatch(make_req("AUTH"), authorized, unused));
  ASSERT_TRUE(authorized);

  ASSERT_EQ(Formatter::err("Client sent AUTH, but no password is set").val, dispatcher.dispatch(make_req("AUTH", "test"), authorized, unused).val);
  ASSERT_TRUE(authorized);

  ASSERT_EQ(Formatter::err("no password is set"), dispatcher.dispatch(make_req("HMAC-AUTH-GENERATE-CHALLENGE", generateSecureRandomBytes(64)), authorized, unused));
  ASSERT_TRUE(authorized);

  ASSERT_EQ(Formatter::err("no password is set"), dispatcher.dispatch(make_req("HMAC-AUTH-VALIDATE-CHALLENGE", generateSecureRandomBytes(64)), authorized, unused));
  ASSERT_TRUE(authorized);
}

TEST(AuthenticationDispatcher, TooSmallPassword) {
  ASSERT_THROW(AuthenticationDispatcher("hunter2"), FatalException);
}

TEST(AuthenticationDispatcher, AuthBasicSanity) {
  AuthenticationDispatcher dispatcher("hunter2_hunter2_hunter2_hunter2_hunter2");
  std::unique_ptr<Authenticator> unused;

  bool authorized = false;
  ASSERT_EQ(Formatter::errArgs("AUTH"), dispatcher.dispatch(make_req("AUTH"), authorized, unused));
  ASSERT_FALSE(authorized);

  ASSERT_EQ(Formatter::err("invalid password"), dispatcher.dispatch(make_req("AUTH", "hunter3"), authorized, unused ));
  ASSERT_FALSE(authorized);

  ASSERT_EQ(Formatter::ok(), dispatcher.dispatch(make_req("AUTH", "hunter2_hunter2_hunter2_hunter2_hunter2"), authorized, unused ));
  ASSERT_TRUE(authorized);
}

TEST(AuthenticationDispatcher, ChallengesBasicSanity) {
  std::string secretKey = "hunter2_hunter2_hunter2_hunter2_hunter2";
  AuthenticationDispatcher dispatcher(secretKey);
  std::unique_ptr<Authenticator> authenticator1, authenticator2;

  bool authorized = false;
  ASSERT_EQ(Formatter::errArgs("HMAC-AUTH-GENERATE-CHALLENGE"), dispatcher.dispatch(make_req("HMAC-AUTH-GENERATE-CHALLENGE"), authorized, authenticator1));
  ASSERT_FALSE(authorized);

  ASSERT_EQ(Formatter::err("no challenge is in progress"), dispatcher.dispatch(make_req("HMAC-AUTH-VALIDATE-CHALLENGE", "asdf"), authorized, authenticator1));
  ASSERT_FALSE(authorized);

  ASSERT_EQ(Formatter::err("exactly 64 random bytes must be provided").val, dispatcher.dispatch(make_req("HMAC-AUTH-GENERATE-CHALLENGE", "1234"), authorized, authenticator1).val);
  ASSERT_FALSE(authorized);

  RedisEncodedResponse resp = dispatcher.dispatch(make_req("HMAC-AUTH-GENERATE-CHALLENGE", generateSecureRandomBytes(64)), authorized, authenticator1);
  ASSERT_FALSE(authorized);

  // parse..
  qclient::ResponseBuilder responseBuilder;
  responseBuilder.feed(resp.val);

  redisReplyPtr rr;
  ASSERT_EQ(responseBuilder.pull(rr), qclient::ResponseBuilder::Status::kOk);

  ASSERT_EQ(rr->type, REDIS_REPLY_STRING);
  std::string challengeString = std::string(rr->str, rr->len);

  resp = dispatcher.dispatch(make_req("HMAC-AUTH-VALIDATE-CHALLENGE", Authenticator::generateSignature(challengeString, secretKey)), authorized, authenticator1);
  ASSERT_EQ(Formatter::ok(), resp);
  ASSERT_TRUE(authorized);
}

qclient::redisReplyPtr strResponse(const std::string &str) {
  qclient::redisReplyPtr reply((redisReply*) malloc(sizeof(redisReply)), freeReplyObject);
  reply->type = REDIS_REPLY_STRING;

  reply->str = (char*) malloc(str.size());
  memcpy(reply->str, str.c_str(), str.size());
  reply->len = str.size();
  return reply;
}

TEST(HmacAuthHandshake, BasicSanity) {
  std::string pw = "hunter2_hunter2_hunter2_hunter2_hunter2";
  HmacAuthHandshake handshake(pw);
  Authenticator authenticator(pw);

  std::vector<std::string> cmd = handshake.provideHandshake();
  ASSERT_EQ(cmd.size(), 2u);
  ASSERT_EQ(cmd[0], "HMAC-AUTH-GENERATE-CHALLENGE");

  qclient::redisReplyPtr reply = strResponse("some-string-to-sign");
  ASSERT_EQ(qclient::Handshake::Status::INVALID, handshake.validateResponse(reply));
  handshake.restart();

  cmd = handshake.provideHandshake();
  ASSERT_EQ(cmd.size(), 2u);
  ASSERT_EQ(cmd[0], "HMAC-AUTH-GENERATE-CHALLENGE");
  std::string challenge = authenticator.generateChallenge(cmd[1]);
  reply = strResponse(challenge);

  ASSERT_EQ(qclient::Handshake::Status::VALID_INCOMPLETE, handshake.validateResponse(reply));
  cmd = handshake.provideHandshake();
  ASSERT_EQ(cmd.size(), 2u);
  ASSERT_EQ(cmd[0], "HMAC-AUTH-VALIDATE-CHALLENGE");
  ASSERT_EQ(authenticator.validateSignature(cmd[1]), Authenticator::ValidationStatus::kOk);

  reply = strResponse("OK");
  reply->type = REDIS_REPLY_STATUS;
  ASSERT_EQ(qclient::Handshake::Status::VALID_COMPLETE, handshake.validateResponse(reply));
}
