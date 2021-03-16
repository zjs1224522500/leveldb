// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  // 完整的 user record
  kFullType = 1,

  // user record 分段后的 record
  // For fragments
  // user record 的第一个 record
  kFirstType = 2,
  //  user record 中间的 record。如果写入的数据比较大，kMiddleType 的 record 可能有多个
  kMiddleType = 3,
  // user record 的最后一个 record
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

// Block 大小 32KB. 32*1024 bytes
static const int kBlockSize = 32768;

// Header 大小 7 bytes
// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
