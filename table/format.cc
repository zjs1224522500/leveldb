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

void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string* dst) const {
  const size_t original_size = dst->size();
  // 首先将metaindex_handle_编码的结果放到dst里面
  // 这里采用的是gooble varint也就是变长整数存放的方式。
  // 由于采用 varint64 进行编码，每个 varint64 最多占用 10 字节
  // 并不是直接将8byte直接复制到dst里面。
  metaindex_handle_.EncodeTo(dst);
  // 接着用同样的方式把index_handle_编码后存放到 dst 里面。
  index_handle_.EncodeTo(dst);
  // resize到40 bytes
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  // 然后把magic number按照8bytes存放下去。
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  // 确保长度是相等的，也就是等于48 bytes.
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice* input) {
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

// 从相应的 SSTable File 中读取给定 Block 位置信息 handle 对应的 Block
// 读取的结果存储在 BlockContents* result
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options,
                 const BlockHandle& handle, BlockContents* result) {

  // 首先初始化读出来的 BlockContents，清空原有的内容
  result->data = Slice();
  // 默认不 Cache
  result->cachable = false;
  // 调用者需要负责释放内存
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  // 获取相应的块大小
  size_t n = static_cast<size_t>(handle.size());
  // 初始化一块包含了 Trailer 的内存区域
  // Trailer 包含相应的 type 和 crc
  char* buf = new char[n + kBlockTrailerSize];
  Slice contents;
  // 从文件指定偏移量读取指定长度的 Block 到 contents 中，buf 作为缓冲区
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);

  // 对读取的结果以及内容的长度进行检查
  if (!s.ok()) {
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // 取出其中的数据并校验
  // Check the crc of the type and the block contents
  const char* data = contents.data();  // Pointer to where Read put the data
  // 配置了校验和的相关检查的情况下，需要进行 crc32 的校验
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  // 看一下是否需要压缩/解压缩
  switch (data[n]) {
    // 没有进行压缩
    case kNoCompression:
      if (data != buf) {
        // 如果contents里面是自带内存的
        // 那么就没有必要使用这个函数内部申请的buf
        // 所以把buf清空掉
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        // 如果contents里面使用了新生成的buf
        // 那么就需要自己去释放内存
        result->data = Slice(buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }

      // Ok
      break;
    // 使用了 snappy 压缩算法  
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      // 取得解压缩之后的结果
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      // 删除压缩的内容
      delete[] buf;
      // 由于使用了新的memory
      // 所以需要caller来释放
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

}  // namespace leveldb
