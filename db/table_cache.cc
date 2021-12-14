// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"

#include "leveldb/env.h"
#include "leveldb/table.h"

#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

/* 查找 file_number 对应的 <table,file >缓存句柄。
 * ldb/sst 都有可能 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  /* file_number -> key */
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  /* 缓存中查找该 key 对应的句柄 */
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    /* db000001.ldb */
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    /* 打开该文件 */
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      /* 打不开则打开 db000001.sst */
      std::string old_fname = SSTTableFileName(dbname_, file_number);

      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      /* 打开该文件 重建 table */
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      /* table|file 关联 */
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      /* 插入缓存 file_number -> TableAndFile  缓存不用这个 tf 了 把它删了 */
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

/* 查找 file_number 对应的 table --> tableptr, 返回 table 的双向迭代器  */
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  /* file_number -> <file, table> */
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  /* 创建迭代器 */
  Iterator* result = table->NewIterator(options);
  /* 迭代器析构的清理函数 refcount -- */
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  /* 查找 file_number 对应的 缓存句柄 */
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    /* 找到  file_number 对应的 table_file 对应的 table */
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    /* 找 k 然后 调用回调函数 handle_result(arg, k, v`) */
    s = t->InternalGet(options, k, arg, handle_result);
    /*  */
    cache_->Release(handle);
  }
  return s;
}

/* 在缓存中清除该 file_number 对应 的 <file, table> */
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
