//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  num_tombstones_ = 0;
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> result_;
  result_.reserve(num_tombstones_);

  for (size_t i = 0; i < num_tombstones_; i++) {
    size_t idx = tombstones_[i];

    KeyType key = KeyAt(idx);

    result_.push_back(key);
  }
  return result_;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return rid_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DeletePair(int idx) {
  BUSTUB_ASSERT(idx >= 0 && idx < GetMaxSize(), "idx is out of bound");
  for (int i = idx; i < GetSize() - 1; i++) {
    rid_array_[i] = rid_array_[i + 1];
    key_array_[i] = key_array_[i + 1];
  }
  ChangeSizeBy(-1);
}
FULL_INDEX_TEMPLATE_ARGUMENTS auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key,
                                                                      const KeyComparator &comparator) const
    -> std::optional<ValueType> {
  int start{0};
  int end{GetSize() - 1};

  while (start <= end) {
    int mid{start + (end - start) / 2};

    if (comparator(key_array_[mid], key) > 0) {
      end = mid - 1;
    } else if (comparator(key_array_[mid], key) < 0) {
      start = mid + 1;
    } else {
      return rid_array_[mid];
    }
  }

  return std::nullopt;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertElement(const KeyType &key, const ValueType &value,
                                               const KeyComparator &comparator) {
  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "can't insert internal page size is full");

  int i = static_cast<int>(GetSize());

  while (i >= 1 && comparator(key_array_[i - 1], key) > 0) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
    i--;
  }

  key_array_[i] = key;
  rid_array_[i] = value;

  ChangeSizeBy(1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs> *receiver,
                                       const page_id_t &rPage_Id) -> KeyType {
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "current size is not max yet");
  std::size_t total{static_cast<std::size_t>(GetSize())};
  std::size_t leftHalfSize{static_cast<std::size_t>(std::floor(total / 2))};
  std::size_t rightHalfSize{total - leftHalfSize};
  std::size_t middleIndex{leftHalfSize};

  for (std::size_t i = leftHalfSize; i < total; i++) {
    receiver->key_array_[i - leftHalfSize] = this->key_array_[i];
    receiver->rid_array_[i - leftHalfSize] = this->rid_array_[i];
  }

  size_t temp_numbstone{0};
  size_t rNumbstone{0};

  for (std::size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] >= leftHalfSize) {
      receiver->tombstones_[rNumbstone] = tombstones_[i] - leftHalfSize;
      rNumbstone++;
    } else {
      tombstones_[temp_numbstone] = tombstones_[i];
      temp_numbstone++;
    }
  }

  num_tombstones_ = temp_numbstone;
  receiver->num_tombstones_ = rNumbstone;

  KeyType middleKey{key_array_[middleIndex]};

  SetSize(leftHalfSize);
  receiver->SetSize(rightHalfSize);

  receiver->SetNextPageId(next_page_id_);
  SetNextPageId(rPage_Id);

  return middleKey;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(B_PLUS_TREE_LEAF_PAGE_TYPE *donor) {
  int size_d{donor->GetSize()};
  int size_r{GetSize()};

  BUSTUB_ASSERT(size_d + size_r <= GetMaxSize(), "combine size exceed maximum size");

  for (int i = size_r; i < (size_d + size_r); i++) {
    key_array_[i] = donor->key_array_[i - size_r];
    rid_array_[i] = donor->rid_array_[i - size_r];
  }

  next_page_id_ = donor->next_page_id_;
  SetSize(size_d + size_r);
  donor->SetSize(0);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKey(const KeyType &key, KeyComparator comparator_) -> bool {
  int start{0};
  int end{GetSize() - 1};

  while (start <= end) {
    int mid{start + (end - start) / 2};

    if (comparator_(key_array_[mid], key) > 0) {
      end = mid - 1;
    } else if (comparator_(key_array_[mid], key) < 0) {
      start = mid + 1;
    } else {
      DeletePair(mid);
      return true;
    }
  }

  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyPos(const KeyType &key, KeyComparator comparator_) const -> int {
  int start{0};
  int end{GetSize() - 1};
  int index = GetSize();
  while (start <= end) {
    int mid{start + (end - start) / 2};

    if (comparator_(key_array_[mid], key) >= 0) {
      index = mid;
      end = mid - 1;
    } else if (comparator_(key_array_[mid], key) < 0) {
      start = mid + 1;
    }
  }

  return index;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
