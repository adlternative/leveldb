// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  /* 计算初始的块偏移量 98301%32768= 32765 */
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  /* 计算该块起始偏移量 98301-32765= 65536 */
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // Don't search a block if we'd be in the trailer
  /* 如果偏移量是最后6字节 肯定不是一条完整的记录，跳到下一个block
   * 32765 > 32768 - 6 则该块的起始偏移量 + 一块 */
  if (offset_in_block > kBlockSize - 6) {
    /*  block_start_location = 98301 + 32768*/
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    /* 跳到起始偏移量 */
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      // BUG(adlternative): no need check?
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  /* 上条记录的偏移量 < 初始化的偏移量  */
  if (last_record_offset_ < initial_offset_) {
    /* 跳到初始化的块开始读
     * 此时 end_of_buffer_offset_ = block_start_location
     */
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false; /* 在零散的记录中
                        当前是否在fragment内，也就是遇到了FIRST 类型的record？*/
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  // 记录我们正在读取的逻辑记录的偏移量
  // 0 是一个让编译器满意的虚拟值
  /* 预期记录偏移量，我们正在读取的[逻辑]record的偏移*/
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    /* 读取到分片中 */
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    // ReadPhysicalRecord 的内部缓冲区中可能只剩下一个空的预告片。
    // 现在计算下一个物理记录的偏移量，正确考虑其标头大小。
    /* 个人觉得这就是当前的记录的初始偏移量 */
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();
    /* 当有init偏移的时候遇到mid需要重新read直到遇到last或并跳到下一个块或者出错*/
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          // 处理早期版本的 log::Writer 中的错误，它可能会在块的尾端发出一个空的
          // kFirstType 记录，然后在下一个块的开头发出一个 kFullType 或
          // kFirstType 记录。
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        /* 获取记录 */
        *record = fragment;
        /* 记录本次的记录的偏移量 */
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        /* 第一段内容都先放到 scratch 中 */
        scratch->assign(fragment.data(), fragment.size());
        /* 将这个标记为真 */
        in_fragmented_record = true;
        break;

      case kMiddleType:
        /* middle 之前 肯定是 middle | first
        所以 in_fragmented_record == true*/
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          /* 继续保存内容 */
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        /* last 之前 肯定是 middle | first
        所以 in_fragmented_record == true*/
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          /* 继续保存内容 */
          scratch->append(fragment.data(), fragment.size());
          /* 放到结果中 */
          *record = Slice(*scratch);
          /* 设置最后一次获得完整记录的偏移量 */
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        /* in_fragmented_record 表示才写到一半呢但最后却写失败 */
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        /* 坏记录汇报 */
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        /* 有问题的记录类型 汇报 */
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

/* 返回最后一条记录的偏移量 */
uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

/* 报告冲突 */
void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

/* 报告冲突 */
void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    /* kHeaderSize =  |checksum:4|length:2|type:1|*/
    /* 0 --> */
    if (buffer_.size() < kHeaderSize) {
      /* 说明没有读完 */
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        /* 读取大小为kBlockSize的块 读取到buffer */
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        /* 游标后移 */
        end_of_buffer_offset_ += buffer_.size();
        /* 读出错（已处理EINTR,如果还有其他的错误会不会是删除了文件之类的？）
         * 报道且eof返回*/
        if (!status.ok()) {
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
          /* 如果读取的大小不足一块 说明读完了 */
        } else if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        /* 读到不完整的header 当作eof */
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    /* 真实数据大小比记录大小短 汇报错误 */
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      /* 如果还没读完，则说明是数据有问题 */
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      /* 如果已经到达文件的结尾却没读到 length 长度的数据，
      猜测是没有写完程序就挂了，则不汇报冲突了。 */
      return kEof;
    }
    /* 零长特判，后面再看啥情况 */
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      // 跳过零长度记录而不报告任何丢弃，因为
      // 此类记录由基于 mmap 的编写代码生成
      // env_posix.cc 预分配文件区域。
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      /* 解码 crc */
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      /* 记录真实数据的 crc  1 + length == |type|data|*/
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        // 删除缓冲区的其余部分，因为“长度”本身可能有
        // 已损坏，如果我们信任它，我们可以找到一些
        // 碰巧看到的真实日志记录的片段
        // 就像一个有效的日志记录。
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    /* 表示消费了 header + data */
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    // 跳过 initial_offset_ 之前开始的物理记录？
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }
    /* 获取数据填写到结果中传出 */
    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
