// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"

#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;  // 2048

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  /* 一开始 filter_index == 0 这里其实不会执行 */
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  /* 似乎是在 starts 里推入 keys 的长度 */
  start_.push_back(keys_.size());
  /* 然后向 keys 中加入 key */
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  //追加数组每个过滤器偏移
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }
  /* 追加 数组偏移 */
  PutFixed32(&result_, array_offset);
  /* 追加 11 */
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  /* 0 key 则记录旧偏移量就好，就不用追加过滤器了 */
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  //从扁平化的密钥结构中生成密钥列表
  // Simplify length computation //简化长度计算
  start_.push_back(keys_.size());
  /* 搞得这么玄乎，其实就是将之前保存在 keys 的字符串中分成出来放到 tmp_keys 中
   减少一个 vector <string> 而已  tmp_keys_ 是个 vector <slice> 开销小 */
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  //为当前的一组键生成过滤器并附加到结果_。
  filter_offsets_.push_back(result_.size());
  /* 将过滤器的序列化结果追加到result后面 */
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);
  /* 清空这些用来傻逼优化的玩样 */
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];  // 11
  uint32_t last_word =
      DecodeFixed32(contents.data() + n - 5); /* array offset */
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;    /* array */
  num_ = (n - 5 - last_word) / 4; /* filter num 因为 filter_offset 4B */
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  /* 算 数据块偏移 block_offset 在过滤器中对应的坐标 index */
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    /* 取出该坐标上的过滤器对应的偏移量 start 然后在 start limit中找 */
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      /* 通过过滤器偏移量找到对应的过滤器 */
      Slice filter = Slice(data_ + start, limit - start);
      /* 在过滤器中找 */
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      /* TODO:这里不会执行吧？ */
      /* 空的过滤器 代表 block_offset 那边没有存 filter 的 key  */
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
