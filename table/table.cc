// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"

#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
};

/* 读取文件 重建 table
此过程中会拿出 footer -> metaindex,index -> filter (index 暂时不用)
*/
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  /* 读 footer */
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  /* 解码 footer */
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  /* 是否检验校验和 */
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  /* 读取 index 块的内容 到 index_block_contents */
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  // TODO(sanjay): 如果footer.metaindex_handle()
  // 大小指示这是一个空块，则跳过此块
  //
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  /* 读取 metaindex 块 到 contents 中  */
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  /* 默认情况喜爱 meta其实就一个项
 key = filter.policy , value = filter 块 offset size  */
  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    /* 找到 <filter.policy, [filterblock.offset,filterblock.size]> */
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  /* <offset,size> 解码失败 */
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  //如果我们开始需要在 Table::Open 中进行校验和验证，
  //我们可能想用 ReadBlock() 统一
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  /* 读 filter block -> block */
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  /* 重建 filter block */
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

/* 删除一个块 from arg */
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

/* 删除一个块 from value */
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

/* 在缓存中删除一个句柄的引用计数 */
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle); /* ref-- */
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
//转换索引迭代器值（即编码的 BlockHandle）
//成对相应块内容的迭代器。
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  /* 将传入的 index_value 解码到 blockhandle 中 */
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  //我们故意在 index_value 中允许额外的东西，以便我们
  //将来可以添加更多功能。
  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      /* key = [table.cache_id, offset] */
      char cache_key_buffer[16];
      /* 编码缓存id */
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      /* 编码块 offset */
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      /* 先在缓存中查找 key */
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        /* 找到则直接拿 */
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        /* 没找到去文件找 */
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          /* 读取到的内容可以缓存 */
          if (contents.cachable && options.fill_cache) {
            /* 添加到缓存中 <[table.cache_id, offset], block>
             * 设置了删除缓存块的清理函数*/
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      /* 无缓存则直接读文件 */
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    /* 一个新的块迭代器 */
    iter = block->NewIterator(table->rep_->options.comparator);
    /* 根据是否缓存注册不同清理函数 */
    if (cache_handle == nullptr) {
      /* 删除块 */
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      /* ref count-- */
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    /* 返回一个错误迭代器 */
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      /* index block iterator */
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

/* 获取 k 对应的数据 kv 并且调用回调 */
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  /* 开一个 index 的块迭代器 */
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  /* 找 k */
  iiter->Seek(k);
  /* 找到了 k 对应的 index 块 */
  if (iiter->Valid()) {
    /* 获取值  index_block value 存放的是 数据块的 [offset, size] */
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    /* 解码到 BlockHandle 中 */
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        /*  过滤器会在对应偏移量的过滤器块中找 key 是否存在 */
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      /* 找到了则从文件读取该块 iiter->value() -> [offset, size] */
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      /* 在该块中查找 k */
      block_iter->Seek(k);
      /* 找到则调用我们的回调函数 */
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

/* 估算一个数据块 key 在 table 中的偏移量 */
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      //奇怪：我们无法解码索引块中的块句柄。
      //我们将只返回元索引块的偏移量，即
      //在这种情况下接近整个文件大小。
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    //键超过文件中的最后一个键。近似偏移
    //通过返回元索引块的偏移量（即就在文件末尾附近）。
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
