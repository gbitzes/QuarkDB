// ----------------------------------------------------------------------
// File: utils.cc
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

#include <gtest/gtest.h>
#include "raft/RaftCommon.hh"
#include "utils/Statistics.hh"
#include "utils/IntToBinaryString.hh"
#include "utils/ParseUtils.hh"
#include "utils/StringUtils.hh"
#include "utils/FileUtils.hh"
#include "utils/Resilvering.hh"
#include "utils/SmartBuffer.hh"
#include "utils/CommandParsing.hh"
#include "utils/TimeFormatting.hh"
#include "utils/Random.hh"
#include "utils/AssistedThread.hh"
#include "utils/CoreLocalArray.hh"
#include "redis/Transaction.hh"
#include "redis/Authenticator.hh"
#include "redis/LeaseFilter.hh"
#include "redis/InternalFilter.hh"
#include "redis/RedisEncodedResponse.hh"
#include "storage/Randomization.hh"
#include "pubsub/SimplePatternMatcher.hh"
#include "pubsub/ThreadSafeMultiMap.hh"
#include "pubsub/SubscriptionTracker.hh"
#include "memory/RingAllocator.hh"
#include "Utils.hh"
#include "Formatter.hh"
#include "qclient/ResponseBuilder.hh"
#include "qclient/QClient.hh"

using namespace quarkdb;

TEST(Utils, binary_string_int_conversion) {
  EXPECT_EQ(intToBinaryString(1), std::string("\x00\x00\x00\x00\x00\x00\x00\x01", 8));
  EXPECT_EQ(binaryStringToInt("\x00\x00\x00\x00\x00\x00\x00\x01"), 1);

  EXPECT_EQ(binaryStringToInt(intToBinaryString(1).data()), 1);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(2).c_str()), 2);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(123415).c_str()), 123415);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(17465798).c_str()), 17465798);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(16583415634).c_str()), 16583415634);
  EXPECT_EQ(binaryStringToInt(intToBinaryString(-1234169761).c_str()), -1234169761);
}

TEST(Utils, binary_string_unsigned_int_conversion) {
  EXPECT_EQ(unsignedIntToBinaryString(1u), std::string("\x00\x00\x00\x00\x00\x00\x00\x01", 8));
  EXPECT_EQ(binaryStringToUnsignedInt("\x00\x00\x00\x00\x00\x00\x00\x01"), 1u);

  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(1u).data()), 1u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(2u).c_str()), 2u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(123415u).c_str()), 123415u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(17465798u).c_str()), 17465798u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(16583415634u).c_str()), 16583415634u);
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(18446744073709551613u).c_str()), 18446744073709551613u);

  uint64_t big_number = std::numeric_limits<uint64_t>::max() / 2;
  EXPECT_EQ(binaryStringToUnsignedInt(unsignedIntToBinaryString(big_number).c_str()), big_number);
}

TEST(Utils, pathJoin) {
  ASSERT_EQ(pathJoin("/home/", "test"), "/home/test");
  ASSERT_EQ(pathJoin("/home", "test"), "/home/test");
  ASSERT_EQ(pathJoin("", "home"), "/home");
  ASSERT_EQ(pathJoin("/home", ""), "/home");
}

TEST(Utils, resilvering_event_parsing) {
  ResilveringEvent event1("f493280d-009e-4388-a7ec-77ce66b77ce9", 123), event2;

  ASSERT_TRUE(ResilveringEvent::deserialize(event1.serialize(), event2));
  ASSERT_EQ(event1, event2);

  ASSERT_EQ(event1.getID(), event2.getID());
  ASSERT_EQ(event1.getStartTime(), event2.getStartTime());

  ResilveringEvent event3("a94a3955-be85-4e70-9fea-0f68eb01de89", 456);
  ASSERT_FALSE(event1 == event3);
}

TEST(Utils, resilvering_history_parsing) {
  ResilveringHistory history;

  history.append(ResilveringEvent("f493280d-009e-4388-a7ec-77ce66b77ce9", 123));
  history.append(ResilveringEvent("a94a3955-be85-4e70-9fea-0f68eb01de89", 456));
  history.append(ResilveringEvent("56f3dcec-2aa6-4487-b708-e867225d849c", 789));

  ResilveringHistory history2;
  ASSERT_TRUE(ResilveringHistory::deserialize(history.serialize(), history2));
  ASSERT_EQ(history, history2);

  for(size_t i = 0; i < history.size(); i++) {
    ASSERT_EQ(history.at(i), history2.at(i));
  }

  history2.append(ResilveringEvent("711e8894-ec4e-4f57-9c2c-eb9e260401ff", 890));
  ASSERT_FALSE(history == history2);

  ResilveringHistory history3, history4;
  ASSERT_TRUE(history3 == history4);
  ASSERT_FALSE(history == history3);
  ASSERT_FALSE(history3 == history);
}

TEST(Utils, replication_status) {
  ReplicationStatus status;
  ReplicaStatus replica { RaftServer("localhost", 123), true, 10000 };

  status.addReplica(replica);
  ASSERT_THROW(status.addReplica(replica), FatalException);

  replica.target = RaftServer("localhost", 456);
  replica.nextIndex = 20000;
  status.addReplica(replica);

  replica.target = RaftServer("localhost", 567);
  replica.online = false;
  status.addReplica(replica);

  ASSERT_EQ(status.replicasOnline(), 2u);
  ASSERT_EQ(status.replicasUpToDate(30000), 2u);
  ASSERT_EQ(status.replicasUpToDate(40001), 1u);
  ASSERT_EQ(status.replicasUpToDate(50001), 0u);

  ASSERT_THROW(status.removeReplica(RaftServer("localhost", 789)), FatalException);
  status.removeReplica(RaftServer("localhost", 456));
  ASSERT_EQ(status.replicasOnline(), 1u);
  ASSERT_EQ(status.replicasUpToDate(30000), 1u);

  ASSERT_EQ(status.getReplicaStatus(RaftServer("localhost", 123)).target, RaftServer("localhost", 123));
#pragma GCC diagnostic ignored "-Wunused-value"
  ASSERT_THROW(status.getReplicaStatus(RaftServer("localhost", 456)).target, FatalException);
}

TEST(Utils, parseIntegerList) {
  std::vector<int64_t> res, tmp;
  ASSERT_TRUE(ParseUtils::parseIntegerList("1,4,7", ",", res));

  tmp = {1, 4, 7};
  ASSERT_EQ(res, tmp);
  ASSERT_FALSE(ParseUtils::parseIntegerList("14 - 7", ",", res));

  ASSERT_TRUE(ParseUtils::parseIntegerList("147", ",", res));
  tmp = {147};
  ASSERT_EQ(res, tmp);
}

template <class T>
class Smart_Buffer : public testing::Test {
protected:
  T buff;
};

typedef ::testing::Types<
  SmartBuffer<1>, SmartBuffer<2>, SmartBuffer<3>, SmartBuffer<4>, SmartBuffer<5>,
  SmartBuffer<6>, SmartBuffer<7>, SmartBuffer<8>, SmartBuffer<9>, SmartBuffer<10>,
  SmartBuffer<11>, SmartBuffer<13>, SmartBuffer<16>, SmartBuffer<20>, SmartBuffer<32>,
  SmartBuffer<100>, SmartBuffer<128>, SmartBuffer<200>, SmartBuffer<333>> Implementations;

TYPED_TEST_CASE(Smart_Buffer, Implementations);

TYPED_TEST(Smart_Buffer, BasicSanity) {
  std::vector<std::string> strings;
  strings.emplace_back("1234");
  strings.emplace_back("adfafasfad2y45uahfdgakh");
  strings.emplace_back("The quick brown fox jumps over the lazy dog");
  strings.emplace_back("1");
  strings.emplace_back(256, 'z');
  strings.emplace_back("3");
  strings.emplace_back(1337, 'y');
  strings.emplace_back(3, 'k');
  strings.emplace_back("what am i doing");
  strings.emplace_back(13, 'f');

  for(size_t i = 0; i < strings.size(); i++) {
    this->buff.resize(strings[i].size());
    memcpy(this->buff.data(), strings[i].c_str(), strings[i].size());
    ASSERT_EQ(this->buff.toString(), strings[i]);
  }
}

TYPED_TEST(Smart_Buffer, Expansion) {
  std::string contents = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris porttitor urna in diam ultricies semper. Vivamus gravida purus eu erat condimentum, ullamcorper aliquam dui commodo. Fusce id nunc euismod mauris venenatis cursus non vel odio. Aliquam porttitor urna eget nibh cursus, eget ultricies quam sagittis. Donec pulvinar fermentum nunc, id rhoncus justo convallis sed. Donec suscipit quis lectus eget maximus. Etiam ut pharetra odio. Morbi ac nulla rhoncus, placerat quam varius, ultrices justo.";

  this->buff.resize(1);
  this->buff[0] = 'L';

  size_t prevSize = 1;

  for(size_t i = 5; i < contents.size(); i++) {
    ASSERT_EQ(prevSize, this->buff.size());

    this->buff.expand(i);

    // ensure old contents are still there!
    ASSERT_EQ(memcmp(this->buff.data(), contents.data(), prevSize), 0);

    // copy over new contents
    memcpy(this->buff.data(), contents.data(), i);

    prevSize = i;
    i += (rand() % 10) + 1;
  }

  this->buff.shrink(2);
  ASSERT_EQ(this->buff.size(), 2u);
}

TEST(StringUtils, CountOccurences) {
  ASSERT_EQ(StringUtils::countOccurences("abc", 'a'), 1u);
  ASSERT_EQ(StringUtils::countOccurences("adfas#abc", '#'), 1u);
  ASSERT_EQ(StringUtils::countOccurences("adfasabc", '#'), 0u);
  ASSERT_EQ(StringUtils::countOccurences("#adfa#sabc#", '#'), 3u);
}

TEST(StringUtils, isPrefix) {
  std::string target = "1234adfas";
  ASSERT_TRUE(StringUtils::isPrefix("1234", target));
  ASSERT_TRUE(StringUtils::isPrefix("1", target));
  ASSERT_TRUE(StringUtils::isPrefix("", target));
  ASSERT_FALSE(StringUtils::isPrefix("2", target));
  ASSERT_FALSE(StringUtils::isPrefix("1234adfasAAA", target));
  ASSERT_FALSE(StringUtils::isPrefix("ldgfkahgfkadgfaksgfkajg", target));
  ASSERT_TRUE(StringUtils::isPrefix("1234adfas", target));
}

TEST(StringUtils, EscapeNonPrintable) {
  ASSERT_TRUE(StringUtils::isPrintable("abc"));
  ASSERT_FALSE(StringUtils::isPrintable("abc\r\n"));

  ASSERT_EQ(StringUtils::escapeNonPrintable("abc" "\xab" "abc"), "abc" "\\xAB" "abc");
  ASSERT_EQ(StringUtils::escapeNonPrintable("abc"), "abc");

  std::string binstr = "abc123";
  binstr.push_back('\0');
  binstr.push_back(0xff);
  binstr += "aaa";

  ASSERT_EQ(StringUtils::escapeNonPrintable(binstr), "abc123\\x00\\xFFaaa");
}

TEST(StringUtils, Base16Encode) {
  ASSERT_EQ(StringUtils::base16Encode("some-text"), "736f6d652d74657874");
  ASSERT_EQ(StringUtils::base16Encode("asdgflhsdfkljh!#$@@$@^SDFA^_^===== ಠ_ಠ"), "61736467666c687364666b6c6a68212324404024405e534446415e5f5e3d3d3d3d3d20e0b2a05fe0b2a0");
  ASSERT_EQ(StringUtils::base16Encode("@!!#$SDFGJSFXBV>?<adsf';l1093 (╯°□°）╯︵ ┻━┻) "), "4021212324534446474a53465842563e3f3c61647366273b6c313039332028e295afc2b0e296a1c2b0efbc89e295afefb8b520e294bbe29481e294bb2920");
}

TEST(StringUtils, RightPad) {
  ASSERT_EQ(StringUtils::rightPad("aaa", 2, ' '), "aaa");
  ASSERT_EQ(StringUtils::rightPad("bb", 4, ' '), "bb  ");
  ASSERT_EQ(StringUtils::rightPad("ccc", 10, '-'), "ccc-------");
}

TEST(ScanParsing, BasicSanity) {
  RedisRequest req { "0" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_TRUE(args.error.empty());
  ASSERT_EQ(args.cursor, "");
}

TEST(ScanParsing, ValidCursor) {
  RedisRequest req { "next:someItem" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_TRUE(args.error.empty());
  ASSERT_EQ(args.cursor, "someItem");
}

TEST(ScanParsing, NegativeCount) {
  RedisRequest req { "next:someItem", "COunT", "-10" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_EQ(args.error, "syntax error");
}

TEST(ScanParsing, NonIntegerCount) {
  RedisRequest req { "next:someItem", "COunT", "adfas" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_EQ(args.error, "value is not an integer or out of range");
}

TEST(ScanParsing, ValidCount) {
  RedisRequest req { "next:someItem", "COunT", "1337" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_TRUE(args.error.empty());
  ASSERT_EQ(args.cursor, "someItem");
  ASSERT_EQ(args.count, 1337);
}

TEST(ScanParsing, WithMatch) {
  RedisRequest req { "next:someItem", "COUNT", "1337", "MATCH", "asdf" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_TRUE(args.error.empty());
  ASSERT_EQ(args.cursor, "someItem");
  ASSERT_EQ(args.count, 1337);
  ASSERT_EQ(args.match, "asdf");
}

TEST(ScanParsing, MultipleMatches) {
  // Behaves just like official redis - with duplicate arguments, the last one
  // takes effect.
  RedisRequest req { "next:someItem", "COUNT", "1337", "MATCH", "asdf", "MATCH", "1234" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_TRUE(args.error.empty());
  ASSERT_EQ(args.cursor, "someItem");
  ASSERT_EQ(args.count, 1337);
  ASSERT_EQ(args.match, "1234");
}

TEST(ScanParsing, EmptySubcommand) {
  RedisRequest req { "next:someItem", "COUNT", "1337", "MATCH", "asdf", "MATCH", "1234", "MATCH" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true);
  ASSERT_EQ(args.error, "syntax error");
}

TEST(ScanParsing, ForbiddenMatch) {
  RedisRequest req { "next:someItem", "COUNT", "1337", "MATCH", "asdf" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), false);
  ASSERT_EQ(args.error, "syntax error");
}

TEST(ScanParsing, MatchLoc) {
  RedisRequest req { "next:someItem", "COUNT", "1337", "MATCHLOC", "asdf" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), false, true);
  ASSERT_TRUE(args.error.empty());
  ASSERT_EQ(args.cursor, "someItem");
  ASSERT_EQ(args.count, 1337);
  ASSERT_EQ(args.matchloc, "asdf");
  ASSERT_TRUE(args.match.empty());
}

TEST(ScanParsing, ForbiddenMatchLoc) {
  RedisRequest req { "next:someItem", "COUNT", "1337", "MATCHLOC", "asdf" };
  ScanCommandArguments args = parseScanCommand(req.begin(), req.end(), true, false);
  ASSERT_EQ(args.error, "syntax error");
}

TEST(TimeFormatting, BasicSanity) {
  using namespace std::chrono;

  auto dur = Years(1) + Months(5) + Days(3) + hours(23) + minutes(45) + seconds(7);
  ASSERT_EQ(formatTime(dur), "1 years, 5 months, 3 days, 23 hours, 45 minutes, 7 seconds");

  dur = Years(2) + Days(6) + hours(20) + minutes(59) + seconds(32);
  ASSERT_EQ(formatTime(dur), "2 years, 6 days, 20 hours, 59 minutes, 32 seconds");

  dur = seconds(61);
  ASSERT_EQ(formatTime(dur), "1 minutes, 1 seconds");

  dur = seconds(60);
  ASSERT_EQ(formatTime(dur), "1 minutes, 0 seconds");

  dur = Years(2) + Days(6) + hours(25) + minutes(59) + seconds(32);
  ASSERT_EQ(formatTime(dur), "2 years, 7 days, 1 hours, 59 minutes, 32 seconds");

  dur = seconds(11299);
  ASSERT_EQ(formatTime(dur), "3 hours, 8 minutes, 19 seconds");
}

TEST(Random, BasicSanity) {
  std::string rnd = generateSecureRandomBytes(5);
  ASSERT_EQ(rnd.size(), 5u);
  qdb_info(StringUtils::base16Encode(rnd));

  rnd = generateSecureRandomBytes(15);
  ASSERT_EQ(rnd.size(), 15u);
  qdb_info(StringUtils::base16Encode(rnd));

  std::string rnd2 = generateSecureRandomBytes(15);
  ASSERT_NE(rnd, rnd2);
}

TEST(Authenticator, BasicSanity) {
  // Test too small secret, verify we throw
  ASSERT_THROW(Authenticator("hunter2"), FatalException);

  // Initialize authenticator with a random pw
  std::string secret = "3614e3639c0a98b1006a50ffe5744f054cf4499592fe8ef1b339601208e80066";
  Authenticator auth(secret);

  std::chrono::system_clock::time_point point(std::chrono::minutes(1333) + std::chrono::milliseconds(333));
  std::string randomBytes("adsfadhfjaldfkjhaldfkjhadflajyqoowortuiwretweortuihlkjghslfgkjhm");
  std::string randomBytes2("adfashflkhjlhjarwqeruityoiy4u5209578osdhklgfjhsfgkljshfgyuwrtoih");

  std::string challenge = auth.generateChallenge(randomBytes2, point, randomBytes);
  ASSERT_EQ(challenge, "adfashflkhjlhjarwqeruityoiy4u5209578osdhklgfjhsfgkljshfgyuwrtoih---79980333---adsfadhfjaldfkjhaldfkjhadflajyqoowortuiwretweortuihlkjghslfgkjhm");
  ASSERT_THROW(auth.generateChallenge(randomBytes, point, randomBytes), FatalException);

  ASSERT_EQ(
    StringUtils::base16Encode(Authenticator::generateSignature("super-secret-message", secret)),
    "1ac4f9c4dd829b0abbe24b7f312480ae0c70c5e17a7104369824744de328a9a7"
  );

  ASSERT_EQ(
    StringUtils::base16Encode(Authenticator::generateSignature("super-secret-message-2", secret)),
    "d70f689ac1ff0035331724a22e72e6de01899c49982d80b1c3eae6640d9d1bc6"
  );

  // Non-sense signature
  challenge = auth.generateChallenge(generateSecureRandomBytes(64));
  ASSERT_EQ(Authenticator::ValidationStatus::kInvalidSignature, auth.validateSignature("aaaaaa"));
  ASSERT_EQ(Authenticator::ValidationStatus::kDeadlinePassed, auth.validateSignature("aaaaaa"));

  // Simulate a timeout
  challenge = auth.generateChallenge(generateSecureRandomBytes(64));
  std::string sig1 = Authenticator::generateSignature(challenge, secret);
  auth.resetDeadline();
  ASSERT_EQ(Authenticator::ValidationStatus::kDeadlinePassed, auth.validateSignature(sig1));

  // Sign correctly
  challenge = auth.generateChallenge(generateSecureRandomBytes(64));
  std::string sig2 = Authenticator::generateSignature(challenge, secret);
  ASSERT_EQ(Authenticator::ValidationStatus::kOk, auth.validateSignature(sig2));

  // Sign using the wrong key
  challenge = auth.generateChallenge(generateSecureRandomBytes(64));
  std::string sig3 = Authenticator::generateSignature(challenge, "hunter2");
  ASSERT_EQ(Authenticator::ValidationStatus::kInvalidSignature, auth.validateSignature(sig3));

  // Something would be terribly wrong if any of the signatures were identical.
  ASSERT_NE(sig1, sig2);
  ASSERT_NE(sig2, sig3);
  ASSERT_NE(sig1, sig3);
}

TEST(Transaction, Parsing) {
  Transaction tx;

  tx.emplace_back("SET", "aaa", "bbb");
  tx.emplace_back("GET", "bbb");

  ASSERT_TRUE(tx.containsWrites());

  tx.setPhantom(false);
  ASSERT_EQ(tx.expectedResponses(), 1);
  tx.setPhantom(true);
  ASSERT_EQ(tx.expectedResponses(), 2);

  PinnedBuffer serialized(tx.serialize());

  Transaction tx2;
  tx2.deserialize(serialized);

  ASSERT_EQ(tx2.size(), 2u);
  ASSERT_EQ(tx2[0], tx[0]);
  ASSERT_EQ(tx2[1], tx[1]);
  ASSERT_EQ(tx, tx2);
  ASSERT_TRUE(tx2.containsWrites());

  Transaction tx3;
  tx3.emplace_back("GET", "aaa");
  ASSERT_FALSE(tx3.containsWrites());
  tx3.emplace_back("HGET", "aaa", "bbb");
  ASSERT_FALSE(tx3.containsWrites());
  tx3.emplace_back("SET", "aaa", "bbb");
  ASSERT_TRUE(tx3.containsWrites());

  ASSERT_EQ(tx3.expectedResponses(), 1);
  tx3.setPhantom(true);
  ASSERT_EQ(tx3.expectedResponses(), 3);

  ASSERT_THROW(tx3.emplace_back("asdf", "1234"), FatalException);
}

TEST(LeaseFilter, BasicSanity) {
  ClockValue timestamp = 567;
  RedisRequest req = {"get", "adsf"};

  ASSERT_THROW(LeaseFilter::transform(req, timestamp), FatalException);

  req = {"lease-acquire", "my-lease", "lease-holder-1234", "10000" };
  LeaseFilter::transform(req, timestamp);

  ASSERT_EQ(req[0], "TIMESTAMPED_LEASE_ACQUIRE");
  ASSERT_EQ(req[1], "my-lease");
  ASSERT_EQ(req[2], "lease-holder-1234");
  ASSERT_EQ(req[3], "10000");
  ASSERT_EQ(req[4], unsignedIntToBinaryString(567));
  ASSERT_EQ(req.getCommand(), RedisCommand::TIMESTAMPED_LEASE_ACQUIRE);

  req = {"lease-get", "my-lease"};
  LeaseFilter::transform(req, timestamp);

  ASSERT_EQ(req[0], "TIMESTAMPED_LEASE_GET");
  ASSERT_EQ(req[1], "my-lease");
  ASSERT_EQ(req[2], unsignedIntToBinaryString(567));
  ASSERT_EQ(req.getCommand(), RedisCommand::TIMESTAMPED_LEASE_GET);
}

TEST(InternalFilter, BasicSanity) {
  RedisRequest req = {"timestamped_lease_get", "asdf" };
  ASSERT_EQ(req.getCommand(), RedisCommand::TIMESTAMPED_LEASE_GET);
  InternalFilter::process(req);
  ASSERT_EQ(req.getCommand(), RedisCommand::INVALID);

  req = {"timestamped_lease_acquire", "asdfas" };
  ASSERT_EQ(req.getCommand(), RedisCommand::TIMESTAMPED_LEASE_ACQUIRE);
  InternalFilter::process(req);
  ASSERT_EQ(req.getCommand(), RedisCommand::INVALID);

  req = {"set", "adsfasf", "qerq"};
  ASSERT_EQ(req.getCommand(), RedisCommand::SET);
  InternalFilter::process(req);
  ASSERT_EQ(req.getCommand(), RedisCommand::SET);
}

TEST(Randomization, BasicSanity) {
  // We use these tests to anchor the hash function, and make sure that in case
  // it accidentally changes (due to different platform, or something) we notice
  // immediatelly when running the tests.
  ASSERT_EQ(getPseudoRandomTag("123"), 7820675105737894236ull);
  ASSERT_EQ(getPseudoRandomTag(""), 15559834046206816424ull);

  // Run the function again, just in case..
  ASSERT_EQ(getPseudoRandomTag("123"), 7820675105737894236ull);
  ASSERT_EQ(getPseudoRandomTag(""), 15559834046206816424ull);

  ASSERT_EQ(getPseudoRandomTag("asdf"), 7195574813216604082ull);
  ASSERT_EQ(getPseudoRandomTag("asdf2"), 8551229147753871701ull);
  ASSERT_EQ(getPseudoRandomTag("test"), 11234724081966486162ull);

  ASSERT_EQ(getPseudoRandomTag("chicken"), 2714014276587970443ull);
  ASSERT_EQ(getPseudoRandomTag("chicken chicken"), 15381190244021194531ull);
  ASSERT_EQ(getPseudoRandomTag("chicken chicken chicken"), 2103198794047051822ull);
}

void changeString(std::string &str) {
  str = "pickles";
}

void nullThread(ThreadAssistant &assistant) {}

void busyWaiting(ThreadAssistant &assistant) {
  while(!assistant.terminationRequested()) {
    // wheeeeeee
  }
}

void coordinator(ThreadAssistant &assistant) {
  AssistedThread t1(busyWaiting);
  AssistedThread t2(busyWaiting);
  AssistedThread t3(busyWaiting);

  t1.setName("busy-waiter-1");
  t2.setName("busy-waiter-2");
  t3.setName("busy-waiter-3");

  // Without the following three lines, we'd block forever waiting for
  // t1 to stop, ignoring our own termination signal.
  assistant.propagateTerminationSignal(t1);
  assistant.propagateTerminationSignal(t2);
  assistant.propagateTerminationSignal(t3);

  t1.blockUntilThreadJoins();
  t2.blockUntilThreadJoins();
  t3.blockUntilThreadJoins();
}

TEST(AssistedThread, CallbackAfterStop) {
  std::string test;

  AssistedThread thread;
  thread.registerCallback(std::bind(changeString, std::ref(test)));
  thread.reset(nullThread);
  thread.join();

  ASSERT_EQ(test, "pickles");
}

TEST(AssistedThread, CoordinatorThread) {
  AssistedThread coord(coordinator);
  coord.join();
}

TEST(RingAllocator, MemoryRegion) {
  std::shared_ptr<MemoryRegion> region = MemoryRegion::Construct(128);
  ASSERT_EQ(region->refcount(), 1);

  ASSERT_EQ(region->size(), 128u);
  ASSERT_EQ(region->bytesFree(), 128u);
  ASSERT_EQ(region->bytesConsumed(), 0u);

  PinnedBuffer ptr1 = region->allocate(8).value();
  ASSERT_EQ(region->refcount(), 2);

  PinnedBuffer ptr2 = region->allocate(16).value();
  ASSERT_EQ(region->refcount(), 3);

  PinnedBuffer ptr3 = region->allocate(3).value();
  ASSERT_EQ(region->refcount(), 4);

  ASSERT_EQ(ptr1.data()+8, ptr2.data());
  ASSERT_EQ(ptr2.data()+16, ptr3.data());
  ASSERT_EQ(region->bytesConsumed(), 27u);
  ASSERT_EQ(region->bytesFree(), 101u);

  region->resetAllocations();

  PinnedBuffer ptr4 = region->allocate(4).value();
  ASSERT_EQ(ptr4.data(), ptr1.data());
  ASSERT_EQ(region->refcount(), 5);

  ASSERT_EQ(region->bytesConsumed(), 4u);
  ASSERT_EQ(region->bytesFree(), 124u);
  ASSERT_FALSE(region->allocate(125u).has_value());

  PinnedBuffer ptr5 = region->allocate(124u).value();
  ASSERT_EQ(ptr4.data() + 4, ptr5.data());
  ASSERT_FALSE(region->allocate(1u).has_value());
  ASSERT_EQ(region->refcount(), 6);

  ASSERT_EQ(region->bytesFree(), 0u);
  ASSERT_EQ(region->bytesConsumed(), 128u);
}

TEST(PinnedBuffer, substr) {
  std::shared_ptr<MemoryRegion> region = MemoryRegion::Construct(128);
  ASSERT_EQ(region->refcount(), 1u);

  PinnedBuffer buf1 = region->allocate(10).value();
  buf1[0] = 'a';
  buf1[9] = 'b';
  for(size_t i = 1; i < 9; i++) {
    buf1[i] = 'c';
  }

  ASSERT_EQ(buf1, "accccccccb");
  ASSERT_EQ(region->refcount(), 2u);

  PinnedBuffer buf2 = buf1.substr(0, 3);
  ASSERT_EQ(region->refcount(), 3u);
  ASSERT_EQ(buf2, "acc");

  PinnedBuffer buf3 = buf1.substr(1, 9);
  ASSERT_EQ(region->refcount(), 4u);
  ASSERT_EQ(buf3, "ccccccccb");

  PinnedBuffer buf4(std::string("qwerty"));
  ASSERT_EQ(buf4, "qwerty");

  PinnedBuffer buf5 = buf4.substr(1, 3);
  ASSERT_EQ(buf5, "wer");
  ASSERT_TRUE(buf5.usingInternalBuffer());
}

TEST(SimplePatternMatcher, BasicSanity) {
  SimplePatternMatcher<int64_t> matcher;
  auto it = matcher.find("aaa");
  ASSERT_FALSE(it.valid());

  ASSERT_EQ(matcher.size(), 0u);
  ASSERT_TRUE(matcher.insert("*", 999));
  ASSERT_EQ(matcher.size(), 1u);
  ASSERT_FALSE(matcher.insert("*", 999));
  ASSERT_EQ(matcher.size(), 1u);
  ASSERT_TRUE(matcher.insert("abc", 111));
  ASSERT_EQ(matcher.size(), 2u);
  ASSERT_TRUE(matcher.insert("bbb", 123));
  ASSERT_EQ(matcher.size(), 3u);

  it = matcher.find("aaa");
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "*");
  ASSERT_EQ(it.getValue(), 999);

  it.next();
  ASSERT_FALSE(it.valid());

  it = matcher.find("abc");
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "*");
  ASSERT_EQ(it.getValue(), 999);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "abc");
  ASSERT_EQ(it.getValue(), 111);

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_TRUE(matcher.insert("[ab]bc", 222));
  ASSERT_EQ(matcher.size(), 4u);
  it = matcher.find("abc");
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "*");
  ASSERT_EQ(it.getValue(), 999);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "[ab]bc");
  ASSERT_EQ(it.getValue(), 222);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "abc");
  ASSERT_EQ(it.getValue(), 111);

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_TRUE(matcher.insert("bbb", 777));
  ASSERT_EQ(matcher.size(), 5u);
  it = matcher.find("bbb");

  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "*");
  ASSERT_EQ(it.getValue(), 999);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bbb");
  ASSERT_EQ(it.getValue(), 123);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bbb");
  ASSERT_EQ(it.getValue(), 777);

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_FALSE(matcher.erase("*", 888));
  ASSERT_EQ(matcher.size(), 5u);
  ASSERT_TRUE(matcher.erase("*", 999));
  ASSERT_EQ(matcher.size(), 4u);

  ASSERT_TRUE(matcher.insert("bb*", 333));
  ASSERT_EQ(matcher.size(), 5u);

  it = matcher.find("bbb");
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bb*");
  ASSERT_EQ(it.getValue(), 333);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bbb");
  ASSERT_EQ(it.getValue(), 123);

  it.next();
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bbb");
  ASSERT_EQ(it.getValue(), 777);

  it.next();
  ASSERT_FALSE(it.valid());

  ASSERT_FALSE(matcher.erase("bb*", 222));
  ASSERT_EQ(matcher.size(), 5u);
  ASSERT_TRUE(matcher.erase("bb*", 333));
  ASSERT_EQ(matcher.size(), 4u);

  it = matcher.find("bbb");
  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bbb");
  ASSERT_EQ(it.getValue(), 123);

  ASSERT_TRUE(it.erase());
  ASSERT_FALSE(it.erase());
  it.next();
  ASSERT_EQ(matcher.size(), 3u);

  ASSERT_TRUE(it.valid());
  ASSERT_EQ(it.getPattern(), "bbb");
  ASSERT_EQ(it.getValue(), 777);

  ASSERT_FALSE(matcher.erase("bbb", 123));
  ASSERT_EQ(matcher.size(), 3u);
  ASSERT_TRUE(matcher.erase("bbb", 777));
  ASSERT_EQ(matcher.size(), 2u);
  ASSERT_TRUE(matcher.erase("[ab]bc", 222));
  ASSERT_EQ(matcher.size(), 1u);
  ASSERT_TRUE(matcher.erase("abc", 111));
  ASSERT_EQ(matcher.size(), 0u);
}

TEST(ThreadSafeMultiMap, BasicSanity) {
  std::vector<size_t> stageSizesToTest = {1, 2, 3, 4, 5, 6, 7, 10, 20, 100, 1000, 2000 };

  ThreadSafeMultiMap<std::string, int64_t> mm;

  auto keyIter = mm.getKeyIterator();
  ASSERT_FALSE(keyIter.valid());

  ASSERT_EQ(mm.size(), 0u);
  ASSERT_TRUE(mm.insert("test", 123));
  ASSERT_EQ(mm.size(), 1u);
  ASSERT_TRUE(mm.insert("test", 234));
  ASSERT_EQ(mm.size(), 2u);
  ASSERT_TRUE(mm.insert("test", 333));
  ASSERT_EQ(mm.size(), 3u);

  for(size_t stageSize : stageSizesToTest) {
    keyIter = mm.getKeyIterator(stageSize);
    ASSERT_TRUE(keyIter.valid());
    ASSERT_EQ(keyIter.getKey(), "test");
    keyIter.next();
    ASSERT_FALSE(keyIter.valid());
  }

  ASSERT_TRUE(mm.insert("test-2", 111));
  ASSERT_EQ(mm.size(), 4u);
  ASSERT_TRUE(mm.insert("test-3", 999));
  ASSERT_EQ(mm.size(), 5u);
  ASSERT_TRUE(mm.insert("test-4", 888));
  ASSERT_EQ(mm.size(), 6u);
  ASSERT_TRUE(mm.insert("test-4", 777));
  ASSERT_EQ(mm.size(), 7u);

  for(size_t stageSize : stageSizesToTest) {
    keyIter = mm.getKeyIterator(stageSize);
    ASSERT_TRUE(keyIter.valid());
    ASSERT_EQ(keyIter.getKey(), "test");
    keyIter.next();

    ASSERT_TRUE(keyIter.valid());
    ASSERT_EQ(keyIter.getKey(), "test-2");
    keyIter.next();

    ASSERT_TRUE(keyIter.valid());
    ASSERT_EQ(keyIter.getKey(), "test-3");
    keyIter.next();

    ASSERT_TRUE(keyIter.valid());
    ASSERT_EQ(keyIter.getKey(), "test-4");
    keyIter.next();

    ASSERT_FALSE(keyIter.valid());
  }

  ASSERT_FALSE(mm.insert("test-2", 111));
  ASSERT_EQ(mm.size(), 7u);

  for(size_t stageSize : stageSizesToTest) {
    auto matchIter = mm.findMatching("test", stageSize);
    ASSERT_TRUE(matchIter.valid());
    ASSERT_EQ(matchIter.getValue(), 123);
    matchIter.next();

    ASSERT_TRUE(matchIter.valid());
    ASSERT_EQ(matchIter.getValue(), 234);
    matchIter.next();

    ASSERT_TRUE(matchIter.valid());
    ASSERT_EQ(matchIter.getValue(), 333);
    matchIter.next();

    ASSERT_FALSE(matchIter.valid());

    matchIter = mm.findMatching("test-3", stageSize);
    ASSERT_TRUE(matchIter.valid());
    ASSERT_EQ(matchIter.getValue(), 999);
    matchIter.next();

    ASSERT_FALSE(matchIter.valid());

    matchIter = mm.findMatching("not-existing", stageSize);
    ASSERT_FALSE(matchIter.valid());
  }

  auto matchIter1 = mm.findMatching("test-2", 1);
  auto matchIter2 = mm.findMatching("test-2", 1);

  ASSERT_TRUE(matchIter1.valid());
  ASSERT_TRUE(matchIter1.valid());

  ASSERT_EQ(matchIter1.getValue(), 111);
  ASSERT_EQ(matchIter2.getValue(), 111);

  ASSERT_TRUE(matchIter1.erase());
  ASSERT_FALSE(matchIter1.erase());

  auto matchIter3 = mm.findMatching("test-2", 1);
  ASSERT_FALSE(matchIter3.valid());
  ASSERT_EQ(mm.size(), 6u);
}

TEST(ThreadSafeMultiMap, FullIteration) {
  std::vector<size_t> stageSizesToTest = {1, 2, 3, 4, 5, 6, 7, 10, 20, 100, 1000, 2000 };
  ThreadSafeMultiMap<std::string, int64_t> mm;

  auto fullIter = mm.getFullIterator();
  ASSERT_FALSE(fullIter.valid());

  mm.insert("aaa", 123);
  mm.insert("aaa", 444);
  mm.insert("aaa", 555);

  mm.insert("bbb", 111);
  mm.insert("bbb", 222);

  mm.insert("ccc", 999);
  mm.insert("ccc", 888);

  mm.insert("ddd", 111);

  fullIter = mm.getFullIterator();
  ASSERT_TRUE(fullIter.valid());

  ASSERT_EQ(fullIter.getKey(), "aaa");
  ASSERT_EQ(fullIter.getValue(), 123);

  ASSERT_TRUE(mm.erase("aaa", 444));
  ASSERT_TRUE(mm.erase("bbb", 111));

  fullIter.next();
  ASSERT_TRUE(fullIter.valid());
  ASSERT_EQ(fullIter.getKey(), "aaa");
  ASSERT_EQ(fullIter.getValue(), 444);

  fullIter.next();
  ASSERT_TRUE(fullIter.valid());
  ASSERT_EQ(fullIter.getKey(), "aaa");
  ASSERT_EQ(fullIter.getValue(), 555);

  fullIter.next();
  ASSERT_TRUE(fullIter.valid());
  ASSERT_EQ(fullIter.getKey(), "bbb");
  ASSERT_EQ(fullIter.getValue(), 222);

  fullIter.next();
  ASSERT_TRUE(fullIter.valid());
  ASSERT_EQ(fullIter.getKey(), "ccc");
  ASSERT_EQ(fullIter.getValue(), 888);

  fullIter.skipKey();
  ASSERT_TRUE(fullIter.valid());
  ASSERT_EQ(fullIter.getKey(), "ddd");
  ASSERT_EQ(fullIter.getValue(), 111);

  fullIter.next();
  ASSERT_FALSE(fullIter.valid());
}

TEST(SubscriptionTracker, BasicSanity) {
  SubscriptionTracker tracker;
  ASSERT_TRUE(tracker.addChannel("test-1"));
  ASSERT_FALSE(tracker.addChannel("test-1"));

  ASSERT_TRUE(tracker.addPattern("test-*"));
  ASSERT_TRUE(tracker.addPattern("test*"));

  ASSERT_TRUE(tracker.hasChannel("test-1"));
  ASSERT_FALSE(tracker.hasChannel("test-2"));

  ASSERT_TRUE(tracker.hasPattern("test-*"));
  ASSERT_FALSE(tracker.hasPattern("test-*1"));
  ASSERT_TRUE(tracker.hasPattern("test*"));

  ASSERT_TRUE(tracker.removeChannel("test-1"));
  ASSERT_FALSE(tracker.removeChannel("test-1"));
  ASSERT_FALSE(tracker.removeChannel("test-2"));

  ASSERT_TRUE(tracker.removePattern("test-*"));
  ASSERT_FALSE(tracker.removePattern("test-*"));
  ASSERT_TRUE(tracker.removePattern("test*"));
  ASSERT_FALSE(tracker.removePattern("test***"));
}

struct alignas(CoreLocal::kCacheLine) AlignedStruct {
  int a;
};

TEST(CoreLocalArray, BasicSanity) {
  CoreLocalArray<AlignedStruct> test;
  std::cout << "CoreLocalArray size: " << test.size() << std::endl;

  std::pair<AlignedStruct*, size_t> local = test.access();
  std::cout << "Executing on core #" << local.second << std::endl;
  local.first->a = 5;

  ASSERT_EQ(test.accessAtCore(local.second)->a, 5);

  // Ensure every single element is aligned
  for(size_t i = 0; i < test.size(); i++) {
    ASSERT_EQ(  ((uintptr_t)test.accessAtCore(i) % CoreLocal::kCacheLine), 0u);
  }
}

TEST(Statistics, BasicSanity) {
  Statistics stats;
  stats.reads = 10;
  stats.writes = 20;
  stats.txreadwrite = 11;

  Statistics stats2;
  stats += stats2;

  ASSERT_EQ(stats.reads, 10);
  ASSERT_EQ(stats.writes, 20);
  ASSERT_EQ(stats.txreadwrite, 11);

  stats2.reads = 1;
  stats2.writes = 2;
  stats2.txreadwrite = 3;

  stats += stats2;
  ASSERT_EQ(stats.reads, 11);
  ASSERT_EQ(stats.writes, 22);
  ASSERT_EQ(stats.txreadwrite, 14);
}

TEST(StatAggregator, BasicSanity) {
  StatAggregator aggr;

  Statistics* stats = aggr.getStats();
  ASSERT_EQ(stats->reads, 0);
  ASSERT_EQ(stats->writes, 0);
  ASSERT_EQ(stats->txreadwrite, 0);

  stats->reads += 10;
  stats->writes += 10;
  stats->txreadwrite += 10;

  Statistics overall = aggr.getOverallStats();
  ASSERT_EQ(overall.reads, 10);
  ASSERT_EQ(overall.writes, 10);
  ASSERT_EQ(overall.txreadwrite, 10);

  Statistics sinceLast = aggr.getOverallStatsSinceLastTime();
  ASSERT_EQ(sinceLast.reads, 10);
  ASSERT_EQ(sinceLast.writes, 10);
  ASSERT_EQ(sinceLast.txreadwrite, 10);

  stats->reads += 30;
  stats->writes += 90;
  stats->txreadwrite += 3;

  sinceLast = aggr.getOverallStatsSinceLastTime();
  ASSERT_EQ(sinceLast.reads, 30);
  ASSERT_EQ(sinceLast.writes, 90);
  ASSERT_EQ(sinceLast.txreadwrite, 3);
}

TEST(HistoricalStatistics, BasicSanity) {
  HistoricalStatistics history(2);

  std::chrono::system_clock::time_point timepoint;
  Statistics stats;

  stats.reads = 90;
  stats.writes = 80;
  timepoint += std::chrono::seconds(100);

  history.push(stats, timepoint);

  stats.reads = 100;
  stats.writes = 50;
  timepoint += std::chrono::seconds(99);

  history.push(stats, timepoint);

  stats.reads = 300;
  stats.writes = 1;
  timepoint += std::chrono::seconds(300);

  history.push(stats, timepoint);

  std::vector<std::string> headers;
  std::vector<std::vector<std::string>> data;
  history.serialize(headers, data);

  qclient::redisReplyPtr ans = qclient::ResponseBuilder::parseRedisEncodedString(Formatter::vectorsWithHeaders(headers, data).val);
  ASSERT_EQ(qclient::describeRedisReply(ans),
    "1) 1) TIMESTAMP 499\n"
    "   2) 1) READS 300\n"
    "      2) WRITES 1\n"
    "      3) TXREAD 0\n"
    "      4) TXREADWRITE 0\n"
    "2) 1) TIMESTAMP 199\n"
    "   2) 1) READS 100\n"
    "      2) WRITES 50\n"
    "      3) TXREAD 0\n"
    "      4) TXREADWRITE 0\n"
  );


}