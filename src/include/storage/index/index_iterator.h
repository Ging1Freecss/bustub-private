//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <optional>
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>;
  // you may define your own constructor based on your member variables
  IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, page_id_t current_pid,
                std::optional<ReadPageGuard> &&read_lk, int index_ = 0);
  IndexIterator() = default;

  ~IndexIterator();  // NOLINT

  // delete copy semantics
  IndexIterator(const IndexIterator &) = delete;
  IndexIterator &operator=(const IndexIterator &) = delete;

  // move semantics
  IndexIterator(IndexIterator &&other) noexcept
      : bpm_{std::exchange(other.bpm_, nullptr)},
        current_pid_{std::exchange(other.current_pid_, INVALID_PAGE_ID)},
        read_lk_{std::exchange(other.read_lk_, std::nullopt)},
        index{std::exchange(other.index, 0)} {}

  IndexIterator &operator=(IndexIterator &&other) noexcept {
    if (this != &other) {
      this->~IndexIterator();
      bpm_ = std::exchange(other.bpm_, nullptr);
      current_pid_ = std::exchange(other.current_pid_, INVALID_PAGE_ID);
      read_lk_ = std::exchange(other.read_lk_, std::nullopt);
      index = std::exchange(other.index, 0);
    }
    return (*this);
  }
  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType, const ValueType>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return (itr.current_pid_ == current_pid_) && (itr.index == index);
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

 private:
  // add your own private member variables here
  std::shared_ptr<TracedBufferPoolManager> bpm_{nullptr};
  page_id_t current_pid_{INVALID_PAGE_ID};
  std::optional<ReadPageGuard> read_lk_{std::nullopt};

  int index{0};
};

}  // namespace bustub
