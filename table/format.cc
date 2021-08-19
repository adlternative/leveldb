// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "leveldb/env.h"

#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

/* 写 <offset,size> */
void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

/* 读 <offset,size> */
Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

/* 写 footer */
void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  /* 写 metaindex <offset,size> */
  /* 写 index <offset,size> */
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  /* TODO: 使用 PutFixed64 */
  /* 写 magic number */
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

/* 读 footer */
Status Footer::DecodeFrom(Slice* input) {
  /* 读魔数 从最后 8B */
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  /* 读 元数据索引 */
  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    /* 读 数据索引 */
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    /* 从 input 去除该 footer */
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

/**
 * @brief 读取数据块
 *
 * @param file 可随机访问的文件
 * @param options
 * @param handle 提供块偏移量和大小
 * @param result 读取结果的存放位置
 * @return Status
 */
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {
  result->data = Slice();
  result->cachable = false; /* 结果不可缓存 */
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  //读取块内容以及 type/crc 页脚。
  //有关构建此结构的代码，请参见 table_builder.cc。
  size_t n = static_cast<size_t>(handle.size());  // 块数据的大小
  char* buf =
      new char[n + kBlockTrailerSize]; /* len(block_content + type + crc) */
  Slice contents;
  /* 从偏移量读取 block_content + type + crc */
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  /* 检查读取大小（一般读磁盘不该读不完） */
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  /* 检查校验和 */
  const char* data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  /* 检查压缩类型 */
  switch (data[n]) {
    case kNoCompression:
      if (data != buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        //如果文件实现给了我们一些其他数据（data|contents）的指针，直接使用它
        //假设当文件打开时 data|contents 将是实时的
        //
        delete[] buf;                  /* 将我们提供的缓冲区释放 */
        result->data = Slice(data, n); /* 存放数据 */
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache 就不缓存它了
      } else {
        result->data = Slice(buf, n); /* 存放数据 */
        result->heap_allocated =
            true; /* 数据是从 buffer 动态分配的 (需要 free) */
        result->cachable = true; /* 结果可缓存 */
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      /* 解压长度 */
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      /* 解压数据 */
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      /* 存放数据 */
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;/* 结果可缓存 */
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
