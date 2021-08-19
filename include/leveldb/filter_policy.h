// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).

#ifndef STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
#define STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_

#include <string>

#include "leveldb/export.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT FilterPolicy {
 public:
  virtual ~FilterPolicy();

  // Return the name of this policy.  Note that if the filter encoding
  // changes in an incompatible way, the name returned by this method
  // must be changed.  Otherwise, old incompatible filters may be
  // passed to methods of this type.
  //返回此策略的名称。注意，如果过滤器编码
  //以不兼容的方式更改，此方法返回的名称
  //必须改变。否则，旧的不兼容过滤器可能
  //传递给这种类型的方法。
  virtual const char* Name() const = 0;

  // keys[0,n-1] contains a list of keys (potentially with duplicates)
  // that are ordered according to the user supplied comparator.
  // Append a filter that summarizes keys[0,n-1] to *dst.
  //
  // Warning: do not change the initial contents of *dst.  Instead,
  // append the newly constructed filter to *dst.
  // keys[0,n-1] 包含一个键列表（可能有重复）
  //根据用户提供的比较器排序。
  //将汇总键 [0,n-1] 的过滤器附加到 *dst。
  //
  //警告：不要更改 *dst 的初始内容。反而，
  //将新构造的过滤器附加到 *dst。
  virtual void CreateFilter(const Slice* keys, int n,
                            std::string* dst) const = 0;

  // "filter" contains the data appended by a preceding call to
  // CreateFilter() on this class.  This method must return true if
  // the key was in the list of keys passed to CreateFilter().
  // This method may return true or false if the key was not on the
  // list, but it should aim to return false with a high probability.

  /* filter" 包含在这个类上调用 CreateFilter() 所附加的数据。如果键在传递给
  CreateFilter() 的键列表中，则此方法必须返回 true。
  如果键不在列表中，此方法可能返回 true 或 false，但它应该旨在以高概率返回
  false。*/
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bits_per_key
// is 10, which yields a filter with ~ 1% false positive rate.
//
// Callers must delete the result after any database that is using the
// result has been closed.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
//返回一个使用布隆过滤器的新过滤策略
//每个键的指定位数。 bits_per_key 的一个很好的值
//是 10，这会产生一个误报率约为 1% 的过滤器。
//
//调用者必须在任何使用该结果的数据库关闭之后删除结果
//
//注意：如果您使用的是忽略某些部分的自定义比较器
//被比较的键，你不能使用 NewBloomFilterPolicy()
//并且必须提供您自己的 FilterPolicy 也忽略
//键的对应部分。例如，如果比较器
//忽略尾随空格，使用 a 是不正确的
//不忽略的 FilterPolicy（如 NewBloomFilterPolicy）
//键中的尾随空格。
LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_FILTER_POLICY_H_
