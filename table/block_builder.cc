// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // 往 Buffer 中写入相应的 restarts 数组，并固定数据类型 Fixed32
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }

  // 最后吸入 restarts 数组的个数，个数其实会受 Entry 大小以及 interval 的影响
  PutFixed32(&buffer_, restarts_.size());

  // 写入完毕之后，修改 finish flag 标志该块写入结束
  finished_ = true;

  // 相应地返回整个 Block 对应的 Buffer
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  // 将 str 转换为 Slice
  Slice last_key_piece(last_key_);
  // 确保还未调用 Finish()
  assert(!finished_);
  // 确保当前计数器小于等于重启点之间的间隔数
  assert(counter_ <= options_->block_restart_interval);

  // 确保当前 buffer 是空的 或者 当前 Key 大于上一个 Key
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);

  // 前缀匹配
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // 获得 shared_bytes，相同前缀的长度
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // 此时为新的重启点，直接写入新的重启点对应的偏移量到 restart 数组
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }

  // 计算非共享长度
  const size_t non_shared = key.size() - shared;

  // 将几个长度对应的变量转为相应的数字类型 VAR32，写入 Buffer
  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // 写入对应的 Key 中非共享的部分以及对应的 Value
  // 至此，一个 Entry 相应地写入完毕
  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // 相应地更新 lastKey
  // Note: 每次前缀匹配都是相邻的两个元素之间的匹配，而不是相对于 restart point
  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);

  // 更新 restart points 之间的计数指针
  counter_++;
}

}  // namespace leveldb
