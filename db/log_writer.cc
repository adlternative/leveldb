// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

/* 初始化 crc的基数 */
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest),
      block_offset_(dest_length % kBlockSize) { /* dest_length % 0x8000 */
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end =
        (left ==
         fragment_length); /* 说明已经剩余需要写的长度已经没有到本块的结尾了。
                            */
    if (begin && end) {
      type = kFullType; /* 说明该块大小 <=32kb */
    } else if (begin) {
      type = kFirstType; /* 说明该块过大，该分片是第一部分 */
    } else if (end) {
      type = kLastType; /* 说明该块过大，且该分片是最后一部分 */
    } else {
      /* 说明该块过大，且该分片是中间的一部分 （占满 32 kb） */
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

/* kHeaderSize =  |checksum:4|length:2|type:1|
 * Data:length
 */
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  /* 首先填充 HEAD */
  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff); /* length [0] */
  buf[5] = static_cast<char>(length >> 8);   /* length [1] */
  buf[6] = static_cast<char>(t); /* type  FULL, FIRST, MIDDLE, LAST*/

  // Compute the crc of the record type and the payload.
  /* 根据当前块的类型，数据内容和大小来计算校验和 */
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  /* 调整以适合存储 */
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);  /* 将校验和写到buf前4个字节 */

  // Write the header and the payload
  /* 写 HEAD */
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    /* 然后写数据 */
    s = dest_->Append(Slice(ptr, length));
    /* 刷磁盘 */
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
