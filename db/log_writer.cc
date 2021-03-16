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
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;


// 写日志
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;

  // begin是从数据的头开始写的么？
  bool begin = true;

  // 循环执行
  do {

    // 检查当前块的剩余容量
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);

    // 剩余的容量不足够容纳一个 header,就直接填 0
    if (leftover < kHeaderSize) {

      // case 1： 如果还剩了点空间
      // Switch to a new block
      if (leftover > 0) {
        // 检查一下 kHeaderSize 是否为 7
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }

      // case 2：如果啥空间也不剩，就不处理了
      // 两种 case 过后要重置偏移量，变成新的块
      block_offset_ = 0;
    }

    // 检查偏移量对了没，就是说剩下的空间必须要放得下一个 header。
    // 要不然是新的块，要不然是能容下一个 header 的块
    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 计算当前块的可用 data 空间，除去了 header
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 看当前空间装不装的下这个 slice
    // 装得下，就直接形成了一个 fragment
    // 装不下，把剩下的数据空间划分为一个 fragment
    const size_t fragment_length = (left < avail) ? left : avail;

    // 根据前面的分段情况设置 record 的类型
    RecordType type;
    const bool end = (left == fragment_length);

    // 如果是从头开始写的，并且又可以直接把slice数据写完。
    // 那么肯定是fullType.
    if (begin && end) {
      type = kFullType;

    // 不能写完，但是是从头开始写  
    } else if (begin) {
      type = kFirstType;

    // 不是从头开始写，但是可以把数据写完
    } else if (end) {
      type = kLastType;

    // 不能从头开始写，也不能把数据写完。
    } else {
      type = kMiddleType;
    }

    // 这里提交一个物理的记录
    // 注意：可能这里并没有把一个slice写完。其实就是写一个 record
    s = EmitPhysicalRecord(type, ptr, fragment_length);

    // 移动写入指针。
    ptr += fragment_length;

    // 需要写入的数据相应减少
    left -= fragment_length;

    // 然后继续写，这时候就不是 slice 开始写的了
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  // 根据前面的代码 fragment_length = min(avail, left)
  // 这里加上这个限制，应该是为了防止后面
  // block_offset_ + kHeaderSize + n 溢出
  assert(length <= 0xffff);  // Must fit in two bytes
  // block_offset_是类成员变量。记录了在一个Block里面的偏移量。
  // block_offset_一定不能溢出。
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);


  // 构建 Header
  // leveldb是一种小端写磁盘的情况
  // LevelDB使用的是小端字节序存储，低位字节排放在内存的低地址端
  // buf前面那个int是用来存放crc32的。4+2+1=7 字节
  // Format the header
  char buf[kHeaderSize];
  // 写入长度: 这里先写入低8位
  buf[4] = static_cast<char>(length & 0xff);
  // 写入长度：再写入高8位
  buf[5] = static_cast<char>(length >> 8);
  // 再写入类型
  buf[6] = static_cast<char>(t);

  // 计算header和数据区的CRC32的值
  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Header 构建完毕，写入 header
  // Write the header and the payload
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {

    // 写入 data
    s = dest_->Append(Slice(ptr, length));

    // 当写完一个record之后，这里就立马flush
    // 但是有可能这个slice并不是完整的。
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  // 相应地移动块内偏移
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
