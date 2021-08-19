// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
//表是从字符串到字符串的排序映射。表是
//不可变且持久。可以安全地访问表
//没有外部同步的多线程。
class LEVELDB_EXPORT Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to nullptr and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  //尝试打开以字节为单位存储的表“文件” [0..file_size)
  //，并读取必要的元数据条目以允许
  //从表中检索数据。
  //
  //如果成功，返回 ok 并将 "*table" 设置为新打开的
  //桌子。不再需要时，客户端应删除“*table”。
  //如果初始化表时出错，设置“*table”
  //到 nullptr 并返回一个非正常状态。不拥有
  //"*source", 但客户端必须确保 "source" 保持有效
  //在返回表的生命周期内。
  //
  //*file 在使用此表时必须保持活动状态。
  static Status Open(const Options& options, RandomAccessFile* file,
                     uint64_t file_size, Table** table);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //在表内容上返回一个新的迭代器。
  // NewIterator() 的结果最初是无效的（调用者必须
  //在使用迭代器之前调用它的 Seek 方法之一）。
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  //给定一个键，返回文件中的近似字节偏移量，其中
  //该键的数据开始（或者如果键是
  //存在于文件中）。返回值以文件形式
  //字节，因此包括基础数据的压缩等效果。
  //例如，表中最后一个键的近似偏移量
  //接近文件长度。
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  friend class TableCache;
  struct Rep;

  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  explicit Table(Rep* rep) : rep_(rep) {}

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  //使用调用后找到的条目调用 (*handle_result)(arg, ...)
  //寻找（键）。如果过滤政策说该键不存在，可能不会调用。
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                     void (*handle_result)(void* arg, const Slice& k,
                                           const Slice& v));

  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);

  Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
