// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include "db/log_format.h"
#include <cstdint>

#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    /* 检测到一些损坏。 “大小”是近似数字
    由于损坏而丢弃的字节数。 */
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-null, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  /*
   * 创建一个从“*file”返回日志记录的Reader。
   * 在使用此Reader时，“*file”必须保持有效。
   * 如果“reporter”不为空，则每当由于检测到损坏而丢弃某些数据时都会通知它。
   * 在使用此Reader时，“*reporter”必须保持活动状态。
   * 如果“校验和”为真，则验证校验和（如果可用）。
   * Reader 将从位于文件内物理位置 >= initial_offset 的第一条记录开始读取。
   */
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  bool ReadRecord(Slice* record, std::string* scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  uint64_t LastRecordOffset();

 private:
  // Extend record types with the following special values
  enum {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    /* 每当我们发现无效的物理记录时返回。
     * 目前出现这种情况有以下三种情况：
     * 该记录具有无效的 CRC（ReadPhysicalRecord 报告丢弃）
     * 记录为0长度记录（无丢包报告）
     * 记录在构造的initial_offset下面（没有报告drop） */
    kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  SequentialFile* const file_;
  Reporter* const reporter_;
  bool const checksum_;
  char* const backing_store_;
  Slice buffer_;  //读取的内容
  bool eof_;      // Last Read() indicated EOF by returning < kBlockSize
              // 上次读的结果是如果返回值<读取块大小则说明EOF

  // Offset of the last record returned by ReadRecord.
  // 函数ReadRecord返回的上一个record的偏移
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  //当前的读取偏移
  uint64_t end_of_buffer_offset_;

  // Offset at which to start looking for the first record to return
  //偏移，从哪里开始读取第一条record
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  // 如果我们在搜索后重新同步（initial_offset_ > 0），则为真。 在
  // 特别是，可以静默运行 kMiddleType 和 kLastType 记录
  // 在此模式下跳过
  bool resyncing_;
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
