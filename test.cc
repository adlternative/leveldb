#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "./include/leveldb/db.h"
using namespace std;

TEST(leveldb, OpenTest) {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  auto path = "/tmp/testdb4";
  ASSERT_TRUE(leveldb::DB::Open(options, path, &db).ok())
      << path << " open failed!";
  delete db;
}

TEST(leveldb, PutTest) {
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/testdb3", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string result_val;
  string random_key = "key" + to_string(rand());
  status = db->Put(leveldb::WriteOptions(), random_key, "hihi");
  if (!status.ok()) {
    printf("put failed! err:%s\n", status.ToString().c_str());
  }

  delete db;
}

TEST(leveldb, GetTest) {
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/testdb3", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string result_val;
  status = db->Put(leveldb::WriteOptions(), "hehe", "hihi");
  if (!status.ok()) {
    printf("put failed! err:%s\n", status.ToString().c_str());
  } else {
    printf("put ok!\n");
  }

  db->Get(leveldb::ReadOptions(), "hehe", &result_val);
  printf("[%s %s]\n", "heheh", result_val.c_str());
  if (!status.ok()) {
    printf("get failed! err:%s\n", status.ToString().c_str());
  } else {
    printf("get ok!\n");
  }
  delete db;
}

TEST(leveldb, PutTest2) {
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/testdb3", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string result_val;
  for (int i = 0; i < 1000000; ++i) {
    string key = to_string(i);
    EXPECT_TRUE(db->Put(leveldb::WriteOptions(), key, "hihi").ok());
  }

  delete db;
}

TEST(leveldb, PutTest3) {
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/testdb13", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string result_val;
  for (int i = 1000000; i >= 0; --i) {
    string key = to_string(i);
    EXPECT_TRUE(db->Put(leveldb::WriteOptions(), key, "hihi").ok());
  }

  delete db;
}

TEST(leveldb, BigKeyPut) {
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/testdb14", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string key(1000000, 'a');
  for (int i = 0; i < 7; i++) {
    key.append(key);
  }
  std::cout << key.size() << std::endl;
  EXPECT_TRUE(db->Put(leveldb::WriteOptions(), key, "hihi").ok());
  // EXPECT_TRUE(db->Get(leveldb::ReadOptions(), key, val).ok());
  delete db;
}

TEST(leveldb, BigKeyGet) {
  leveldb::DB* db;
  leveldb::Options options;

  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/testdb14", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string key(1000000, 'a');
  for (int i = 0; i < 7; i++) {
    key.append(key);
  }
  std::cout << key.size() << std::endl;

  string val;
  auto rc = db->Get(leveldb::ReadOptions(), key, &val);
  EXPECT_TRUE(rc.ok()) << rc.ToString() << std::endl;
  EXPECT_EQ(val, "hihi");
  delete db;
}

TEST(leveldb, PutTestWithFilter) {
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/fudb", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  status = db->Put(leveldb::WriteOptions(), "hehe2", "hihi2");
  if (!status.ok()) {
    printf("put failed! err:%s\n", status.ToString().c_str());
  } else {
    printf("put ok!\n");
  }

  delete db;
}

TEST(leveldb, GetTestWithFilter) {
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.create_if_missing = true;
  auto status = leveldb::DB::Open(options, "/tmp/fudb", &db);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    return;
  }

  string result_val;

  db->Get(leveldb::ReadOptions(), "hehe", &result_val);
  printf("[%s %s]\n", "heheh", result_val.c_str());
  if (!status.ok()) {
    printf("get failed! err:%s\n", status.ToString().c_str());
  } else {
    printf("get ok!\n");
  }
  delete db;
}

TEST(leveldb, PutAndDelete) {
  leveldb::DB* db;
  leveldb::Options options;
  string key = "key";
  string value = "value";

  options.create_if_missing = true;
  auto rc = leveldb::DB::Open(options, "/tmp/testdb3", &db);
  EXPECT_TRUE(rc.ok()) << rc.ToString() << std::endl;

  string result_val;
  rc = db->Put(leveldb::WriteOptions(), key, value);
  EXPECT_TRUE(rc.ok()) << rc.ToString() << std::endl;

  rc = db->Get(leveldb::ReadOptions(), key, &result_val);
  EXPECT_TRUE(rc.ok()) << rc.ToString() << std::endl;
  printf("[%s %s]\n", key.c_str(), result_val.c_str());

  rc = db->Delete(leveldb::WriteOptions(), key);
  EXPECT_TRUE(rc.ok()) << rc.ToString() << std::endl;
  rc = db->Get(leveldb::ReadOptions(), key, &result_val);
  EXPECT_TRUE(rc.IsNotFound()) << rc.ToString() << std::endl;
  delete db;
}