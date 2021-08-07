// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

// 线程安全
// -------------
//
// 写入需要外部同步，很可能是互斥锁。 读取需要保证在读取过程中不会销毁
// SkipList。 除此之外，读取过程没有任何内部锁定或同步。
//
// 不变量：
//
// (1) 在 SkipList 销毁之前，永远不会删除已分配的节点。
// 这一点由代码保证，因为我们从不删除任何跳过列表节点。
//
// (2) Node 链接到 SkipList 后，除了 next/prev 指针，Node 的内容是不可变的。只有
// Insert() 修改列表，初始化节点和使用 release-stores 时要小心
// 在一个或多个列表中发布节点。

// ... prev 与 next 指针排序 ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  // 创建一个新的 SkipList 对象，它将使用“cmp”来比较键，
  // 并将使用“*arena”分配内存。 在 arena 中分配的对象
  // 必须在跳过列表对象的生命周期内保持分配状态。
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  /* 列表中当前没有与 key 相等的内容 */
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  // 如果与 key 比较的条目在列表中，则返回 true。
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  // 跳表的迭代器
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    // 前进到第一个键 >= target 的条目
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    // 定位在列表中的第一个条目。
    // 如果列表不为空, 迭代器的最终状态是 Valid() 。
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    // 定位在列表中的最后一个条目。
    // 如果列表不为空, 迭代器的最终状态是 Valid() 。
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
    //有意设计的本类可复制
  };

 private:
  enum { kMaxHeight = 12 };

  // 返回最高高度
  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  // 返回出现在 key 处或之后的最早节点。
  // 如果没有这样的节点，则返回 nullptr。
  //
  // 如果 prev 不为空，则用指向 [0..max_height_-1]
  // 中每一层的“level”上一个节点的指针填充 prev[level]。
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    // 使用“获取加载”，以便我们观察返回节点的完全初始化版本。
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    // 使用“发布存储”，以便任何读取此内容的
    // 指针观察插入节点的完全初始化版本。
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  /* 分配 | Node | (height - 1) * Node* | */
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  /* x 蕴含着一种我恰好比你小的意思，之后会存到 prev 中 */
  Node* x = head_;
  /* GetMaxHeight 初始为 1 */
  int level = GetMaxHeight() - 1;
  /* 从最高层开始 */
  while (true) {
    Node* next = x->Next(level);
    /* 如果 next < key  更新 x */
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      // 继续向后找
      x = next;
    } else {
      // !next || next >= key
      // [level, 0]层 设置 prev
      if (prev != nullptr) prev[level] = x;
      /* 一直到最低层才返回 next */
      // 返回时 next 恰好大于等于 key
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

/*  find x if x < key < x->next and level = 0 */
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    /* x < key */
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    /* x->next >= key */
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        /* 越下层跨度越小 x->next 越接近恰好小 */
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

/* 一路从 head_ 从最高层开始找到最后一层就好 */
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  /* 越高的层级跨度越大，一路从 head_ 从最高层开始找到最后一层就好 */
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

/* 插入 key 和将它的层级和其它节点的层级连接起来 */
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  // TODO(opt): 我们可以使用 FindGreaterOrEqual() 的无障碍变体
  // 这里因为 Insert() 是外部同步的。

  /* prev[] 是用来表示每一层应当连接到该节点的前一个节点组成的数组 */
  Node* prev[kMaxHeight];
  /* 寻找比 key 要大的节点 x , 设置低层的 prev*/
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  // 它说它的数据结构不允许重复插入相同的 key
  assert(x == nullptr || !Equal(key, x->key));
  /* 小的概率大，大的概率小 */
  int height = RandomHeight();
  /* 初始值为1 如果随机高度更高的话，则让 prev 的这些[oldheight,
   * newheight] 高层设置为 head */
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    /* 可以在不与并发读者同步的情况下改变 max_height_ 。 观察 max_height_
     * 新值的并发阅读器将看到来自 head_ (nullptr)
     * 的新级别指针的旧值，或在下面的循环中设置的新值。
     * 在前一种情况下，读取器将立即下降到下一个级别，因为 nullptr
     * 在所有键之后排序。 在后一种情况下，读者将使用新节点。 */
    max_height_.store(height, std::memory_order_relaxed);
  }
  /* 这似乎是在重用 x? (说白了前面取出 x 只是检验了一下没有啥用) */
  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    /* x->next =prev->next */
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    /* prev->next=x */
    prev[i]->SetNext(i, x);
  }
}

/* 寻找 key */
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
