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
    // 释放filter 即FilterBlockReader
    delete filter;
    // 释放filter的数据部分
    delete[] filter_data;
    // 释放data block index
    delete index_block;
  }

  // 传进来的选项
  Options options;
  // 状态
  Status status;
  // 随机访问的文件
  RandomAccessFile* file;
  // 缓存中的id
  uint64_t cache_id;
  // filter block
  // 按照filter block的格式将filter读出来
  // 形成一个真正的filter
  // 从代码的角度上讲，读出来之后，就直接是一个可用的filter了
  FilterBlockReader* filter;
  // filter需要使用到的数据
  const char* filter_data;

  // Meta Index Block Handle
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
 // Index Block
  Block* index_block;
};

// 工厂类，这里将table传进来，然后将结果放到table里面传回去。
// 总结一下：open的时候，只会把data index block 和 meta block 读出来。
// 这是因为data index block里面存有key的信息，可以直接用来进行检索
// meta index block 里面只有一个 filter.name 和 offset/size
// 存放 meta index block 是没有什么意义的。
// 所以为了简便起见，也是为了压缩内存,这里只存放了
// - data index block
// - meta block
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  // 初始化对应的 Table 指针
  *table = nullptr;
  // 文件的指定范围如果比 Footer 还小，报错
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  // 创建一个 Footer 大小的空间来容纳 Footer
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  // 从 Footer 所在的偏移量开始读取，数据存储在 footer_input 中
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  // 解码（反序列化）出 Footer，从而得到包含了 MetaIndex 和 DataIndex 两个 Block 的 BlockHandle
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  // 根据 IndexBlockHandle 读取出对应的 Index Block
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    // 转换成 Index Block
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;

    // 取出 Meta Index Block
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;

    // 判断是否开其 Block Cache，开启则生成一个 ID
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);

    // 开始取出meta block，从而设置 filter
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    // 构造 Table
    *table = new Table(rep);
    // 读取 Meta Block
    (*table)->ReadMeta(footer);
  }

  return s;
}

// 这个函数的主要作用有两个：
// 1. 通过footer读出meta block index
// 2. 取出meta block index
// 3. 读出meta block的真正内容
void Table::ReadMeta(const Footer& footer) {

  // 如果配置中未配置 filter，则不进行 filter 的相关配置
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  // 读取对应的 Meta Block Index
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }

  // 利用contents生成meta block index
  // meta block index的格式是
  // | filter.name | BlockHandle | 
  // | compresstype 1 byte       | 
  // | crc32 4 byte              |
  Block* meta = new Block(contents);

  // 从 MetaBlock 中获取对应的 filter 数据
  Iterator* iter = meta->NewIterator(BytewiseComparator());

  // 生成 filter 的名字
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  // 这里的key就是filter.xxName
  // value就是BlockHandle
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {

    // 得到BlockHandle之后，去读出filter block
    // filter block也就是meta block
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

// 当得到filter block的offset/size之后，把
// filter block 即 meta block读出来
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;

  // 由于传进来的handle是压缩表示，所以这里需要解码
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  // crc 校验
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }

  // 从文件的偏移处 filter_handle 把内容，从文件中读到内存里面 block
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  // 如果ReadBlock里面申请了这段内存
  // 那么后面是需要释放这段内存的
  if (block.heap_allocated) {
    // 指向原始的数据
    // block.data是一个slice
    rep_->filter_data = block.data.data();  // Will need to delete later
  }

  // 生成相应的filter
  // 虽然名字是叫Reader
  // 但是这个BlockReader本质上就是一个filter
  // 可以用来过滤key
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

// 删除一个 Block
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

// 删除cache中的block内存
// 这个主要是用在当cache中的item被删除的时候，会被自动调用
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

// 从cache中移出去
// 相当于是从map<x,Y>中移除一个item
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// 这里是把一个编码过的BlockHandle指向的data block里面的data(key/value)部分读出来
// arg 就是一个Table对象
// index_value 就是一个编码过的 offset & size
// 真正的读取操作由 ReadBlock 底层来完成。
// 而这个函数主要是操作与 Table & Block Cache 相关的事情。
// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  // arg就是table，那么为什么不直接命名为Table *arg
  // 这里是为了添加到cache的时候方便
  Table* table = reinterpret_cast<Table*>(arg);
  // block_cache指的就是对sst文件中的data block部分的cache
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  // 取出offset/size
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    // 这里生成一段内存，存放block contents
    BlockContents contents;
    // 如果block cache不为空
    if (block_cache != nullptr) {
      // 把cache_id/offset编码之后，放到cache_key_buffer
      // 这里是把cache_key_buffer当成一个cache的index
      // 如果是把cache当成一个map<cache_key, value>
      // 那么cache_key_buffer就是前面的key
      // value就是相应的block指针

      // 注意block cache里面索引的生成方式
      char cache_key_buffer[16];

      // 当前table是有一个id的。
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());

      // 下面这段逻辑代码虽然看起来很长，逻辑如下：
      // 1. 从cache中看一下是否可以找到相应的block
      //   a. 找到，那么直接返回
      //   b. 没有找到，那么就从文件中读取相应的block到contents里面。
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));

      // 看一下是否已经在block cache里面了。
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        // 如果能够找到
        // return reinterpret_cast<LRUHandle*>(handle)->value;
        // 其实这里value部分就是一个Block*
        // cache.cc:695   virtual void* Value(Handle* handle) {
        //           return reinterpret_cast<LRUHandle*>(handle)->value;
        //       }
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        // 如果没有在table cache中找到
        // 那么读到contents里面
        // contents就是一段内存
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          // 利用contents这段内存来生成block.
          // 注意：内存仍然是共享的
          block = new Block(contents);
          // 如果是需要加到cache中。
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      // 如果根本就没有开cache，那么也就不用想了
      // 直接从文件中把相应的内容读到contents里面
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    // 这里生成相应的Iterator，并且会注册删除时的回调函数
    iter = block->NewIterator(table->rep_->options.comparator);
    // cache_handle是cache中的句柄
    if (cache_handle == nullptr) {
      // 所有的Iterator都可以注册这个回调函数，在退出的时候，自动执行清理工作。
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 这里生成一个二级的Iterator
// 内部是一个block iterator
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

// 这个函数只有一个地方用到了，那就是
// ./db/table_cache.cc:119:    s = t->InternalGet(options, k, arg, handle_result);
// 这个函数的作用就是：找到相应的key/value之后，然后用 handle_result 来进行一次函数的回调
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  // 生成 data block index 的 Iterator
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  // Iterator 移动到 k
  iiter->Seek(k);
  // 如果这个iter是有效的
  if (iiter->Valid()) {
    // 这里需要注意一下data block index的格式
    // | split_key | blockHandle|
    // 所以这里取出来的是handle，也就是拿到了offset,size
    Slice handle_value = iiter->value();
    // 这里拿到filter
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // 如果有filter，那么看一下相应的key是否存在
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // filter的策略是：如果回答不存在，那是肯定没有
      // 如果回答有，但是有可能找不到
      // 这里是发现不存在，那么肯定是不存在了。
      // Not found
    } else {
      // 如果没有filter，那么就需要去block里面找了
      // 当给定offset/size之后，就可以把一个完整的block构建出来了
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      // 然后利用这个block的iterator移动到k这里。
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        // 如果iter是有效的
        // 那么执行传进来的回调函数
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      // 返回iter的状态
      s = block_iter->status();
      delete block_iter;
    }
  }
  // 如果状态是ok的，那么看iiter的状态
  if (s.ok()) {
    s = iiter->status();
  }
  // 清除data block index的iterator
  delete iiter;
  return s;
}

// 这个函数是查找key最接近的offset
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  // 取得data block index里面的iterator
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  // seek到key边上
  index_iter->Seek(key);
  uint64_t result;
  // 这个index_iter是否还有效?
  if (index_iter->Valid()) {
    BlockHandle handle;
    // 如果有效，取offset/size
    Slice input = index_iter->value();
    // 解码
    Status s = handle.DecodeFrom(&input);
    // 取出offset
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      // 这里有意思的是，如果找不到，那么返回的是
      // meta block index index
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    // 同理，返回meta block index index
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
