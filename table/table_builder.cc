// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.

  /* 在我们看到下一个数据块的第一个键之前，我们不会发出一个块的索引条目。
  这允许我们在索引块中使用较短的键。 例如，考虑键“the quick brown fox”和“the
  who”之间的块边界。 我们可以使用“the r”作为索引块条目的键，因为
        the quick brown fox < the r < the who
  不变式：r->pending_index_entry 仅当 data_block 为空时才为真。 */
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block
                               //添加到索引块的句柄

  std::string compressed_output; /* 压缩输出 （就是一个临时的缓冲区而已）*/
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  /* 保证 key 升序 */
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  /* Flush 则走这个分支 */
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    /* 将 last_key 修改成和 key 公有前缀 + 第一个不同 + 1 */
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    /* 写 上一个数据块的 offset,size 到 handle_encoding */
    r->pending_handle.EncodeTo(&handle_encoding);
    /* 在索引块中记录 "公有部分 + 1" + offset,size */
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }
  /* 过滤器块 记录 key */
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }
  /* last_key = key */
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  /* 数据块 添加 key value */
  r->data_block.Add(key, value);
  /* 估计数据块大小 */
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  /* 如果当前数据块大于 4k 则 Flash 刷磁盘 */
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  /* 写完该块了 pending_handle 获取该数据块的 offset,size */
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    /* 写完后将这个 pending_index_entry 设置为 true (下次 Add 就得更新索引了) */
    r->pending_index_entry = true;
    /* 可能上次没有刷磁盘，这次则试试 */
    r->status = r->file->Flush();
  }
  /* 添加过滤器新偏移位置 */
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

/* 压缩并写 block ，handle 记录写入块的 offset,size */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  /* 形成一个 data block */
  Slice raw = block->Finish();
  /* 需要写的块数据 */
  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      /* 无压缩则写原始数据 */
      block_contents = raw;
      break;

    case kSnappyCompression: {
      /* 压缩 */
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  /* 开始写 block crc compress_type ... handle记录 数据块的 offset,size */
  WriteRawBlock(block_contents, type, handle);
  /* 清空压缩的输出 */
  r->compressed_output.clear();
  /* 重新初始化该数据块 */
  block->Reset();
}

/**
 * @brief 从文件中写 block_contents
 *
 * @param block_contents 写入的数据
 * @param type  压缩类型
 * @param handle 记录块的 offset,size
 *
 * handle = |offset|block_size|
 *         rawblock = |block_contents|compress_type:1|crc:4|
 *
 */
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  /* 在块句柄中记录 <offset, block_size> */
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  /* 缓冲区追加 块内容 block_contents，可能会触发写磁盘 */
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    /* trailer[0] 压缩类型 */
    trailer[0] = type;
    /* 计算 block_contents 的校验和 */
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    /* crc 再算个 trailer[0] 也就是压缩的类型那个 */
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    /* trailer[1,4] crc */
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    /* 向文件追加 trailer */
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    /* 更新偏移量 */
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  /* 将过滤器块无压缩写入 */
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      /* key = filter.policy , value = filter 块 offset size  */
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    /* 目前元数据只有 布隆过滤器 */
    /*  写入元数据索引该块 */
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      /* 说白了就是 key[0]+1 (如果是 BytewiseComparatorImpl)*/
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    /* 设置 metaindex */
    footer.set_metaindex_handle(metaindex_block_handle);
    /* 设置 index */
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    /* 向文件写 */
    r->status = r->file->Append(footer_encoding);
    /* 更新偏移 */
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

/* 设置 closed */
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
