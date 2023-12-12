//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetBucketSize() const -> int {
  return bucket_size_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  auto pos = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[pos];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  auto pos = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[pos];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // todo! use more efficient concurrent method
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  auto pos = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[pos];
  LOG_INFO("insert with globalDepth: %d", GetGlobalDepth());
  // insert success
  if (bucket->Insert(key, value)) {
    return;
  }
  // split bucket
  if (bucket->GetDepth() != GetGlobalDepth()) {
    LOG_INFO("bucket split");
    int local_high_bit = (1 << bucket->GetDepth());
    auto first_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    auto second_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    for (const auto &elem : bucket->GetItems()) {
      int p = IndexOf(elem.first);
      if ((p & local_high_bit) != 0) {
        first_bucket->Insert(elem.first, elem.second);
      } else {
        second_bucket->Insert(elem.first, elem.second);
      }
    }
    for (size_t i = pos & (local_high_bit - 1); i < dir_.size(); i += local_high_bit) {
      if ((i & local_high_bit) != 0) {
        dir_[i] = first_bucket;
      } else {
        dir_[i] = second_bucket;
      }
    }
    num_buckets_++;
    Insert(key, value);
    return;
  }
  LOG_INFO("[start][add global_depth]");
  // add global_depth & reorder KV
  int num = 1 << global_depth_;
  global_depth_++;
  dir_.resize(1 << global_depth_);
  for (int i = num; i < (num << 1); i++) {
    dir_[i] = dir_[i & (num - 1)];
  }
  LOG_INFO("[end][add global_depth]");
  Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth), list_() {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &elem : list_) {
    if (elem.first == key) {
      value = elem.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  bool flag = false;
  list_.remove_if([&](const std::pair<K, V> &pair) {
    if (pair.first == key) {
      flag = true;
      return true;
    }
    return false;
  });
  return flag;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if ((*it).first == key) {
      *it = std::make_pair(key, value);
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
