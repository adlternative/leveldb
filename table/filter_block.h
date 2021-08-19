// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"

#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish

//一个 FilterBlockBuilder 用于为一个指定的表构造所有的过滤器块。
//它生成表中一个存储为单个字符串的特殊块。
//
//对 FilterBlockBuilder 的调用顺序必须与正则表达式匹配：
//(StartBlock AddKey*)*完成
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_; /* 过滤器协议 */
  std::string keys_;           // Flattened key contents 压平的 key 内容
  std::vector<size_t> start_;  // Starting index in keys_ of each key
                               // 每个键的keys_中的起始索引
  std::string
      result_;  // Filter data computed so far //过滤到目前为止计算的数据
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
                        // 过滤数据的指针（在块开始时）
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
                        // 指向偏移数组开始的指针（在块结束处）
  size_t num_;  // Number of entries in offset array //偏移数组中的条目数
  size_t base_lg_;  // Encoding parameter (see kFilterBaseLg in .cc file)
                    //编码参数（参见 .cc 文件中的 kFilterBaseLg）
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
