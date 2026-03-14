//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>
#include <optional>
#include "common/config.h"

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, page_id_t current_pid,
                                  std::optional<ReadPageGuard> &&read_lk, int index_)
    : bpm_{bpm}, current_pid_{current_pid}, read_lk_{std::move(read_lk)}, index{index_} {};

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return (current_pid_ == INVALID_PAGE_ID); }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType, const ValueType> {
  const LeafPage *leaf_node{read_lk_->As<LeafPage>()};
  auto key = leaf_node->KeyAt(index);
  auto rid = leaf_node->ValueAt(index);

  return std::pair<const KeyType, const ValueType>{key, rid};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  const LeafPage *leaf_node{read_lk_->As<LeafPage>()};
  if (index < leaf_node->GetSize() - 1) {
    index++;
    return (*this);
  }
  page_id_t next_pid{leaf_node->GetNextPageId()};

  if (next_pid != INVALID_PAGE_ID) {
    index = 0;
    read_lk_ = bpm_->ReadPage(next_pid);
    current_pid_ = read_lk_->GetPageId();
    return (*this);
  }

  index = 0;
  read_lk_ = std::nullopt;
  current_pid_ = INVALID_PAGE_ID;
  return (*this);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
