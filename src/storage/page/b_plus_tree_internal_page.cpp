//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set max page size,
 * must be called after the creation of a new page to make a valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 1, "index is less than 1");
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 1, "index is less than 1");
  key_array_[index] = key;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator comparator) const -> ValueType {
  int start{1};
  int end{GetSize() - 1};
  int target{0};

  while (start <= end) {
    int mid{start + (end - start) / 2};

    if (comparator(key_array_[mid], key) > 0) {
      end = mid - 1;
    } else {
      start = mid + 1;
      target = mid;
    }
  }

  return page_id_array_[target];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { page_id_array_[index] = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *receiver,
                                                const KeyType &key, const ValueType &value,
                                                const KeyComparator comparator_) -> KeyType {
  BUSTUB_ASSERT(GetSize() == GetMaxSize(), "current size is not max yet");
  std::size_t total{static_cast<std::size_t>(GetSize()) + 1};

  std::vector<std::pair<KeyType, ValueType>> temp_buffer(total);
  temp_buffer.reserve(total);

  temp_buffer[0].second = page_id_array_[0];

  for (int i = 1; i < GetSize(); i++) {
    temp_buffer[i].first = key_array_[i];
    temp_buffer[i].second = page_id_array_[i];
  }

  int i{GetSize()};

  while (i >= 2 && comparator_(temp_buffer[i - 1].first, key) > 0) {
    temp_buffer[i] = temp_buffer[i - 1];
    i--;
  }

  temp_buffer[i] = std::pair{key, value};

  std::size_t splitIndex{(total + 1) / 2};
  KeyType middle_key{temp_buffer[splitIndex].first};

  for (int k = 1; k < static_cast<int>(splitIndex); k++) {
    key_array_[k] = temp_buffer[k].first;
    page_id_array_[k] = temp_buffer[k].second;
  }
  page_id_array_[0] = temp_buffer[0].second;

  int receiverSize{0};
  for (int k = splitIndex + 1; k < static_cast<int>(total); k++) {
    int idx{k - static_cast<int>(splitIndex)};
    receiver->key_array_[idx] = temp_buffer[k].first;
    receiver->page_id_array_[idx] = temp_buffer[k].second;
    receiverSize++;
  }

  receiver->page_id_array_[0] = temp_buffer[splitIndex].second;
  receiverSize++;

  receiver->SetSize(receiverSize);
  SetSize(splitIndex);

  return middle_key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertElement(const KeyType &key, const ValueType &value,
                                                   KeyComparator comparator) {
  BUSTUB_ASSERT(GetSize() < GetMaxSize(), "can't insert internal page size is full");

  int i = static_cast<int>(GetSize());

  while (i >= 2 && comparator(key_array_[i - 1], key) > 0) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
    i--;
  }

  key_array_[i] = key;
  page_id_array_[i] = value;

  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value, const KeyComparator comparator) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }

  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(B_PLUS_TREE_INTERNAL_PAGE_TYPE *donor, KeyType middleKey) {
  int size_r{GetSize()};
  int size_d{donor->GetSize()};
  BUSTUB_ASSERT(size_d + size_r <= GetMaxSize(),
                "in merge function of internal page, size is not small enough for merge");

  SetKeyAt(size_r, middleKey);
  SetValueAt(size_r, donor->ValueAt(0));

  for (int i = 1; i < size_d; i++) {
    SetKeyAt(i + size_r, donor->KeyAt(i));
    SetValueAt(i + size_r, donor->ValueAt(i));
  }

  SetSize(size_r + size_d);
  donor->SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeletePair(int idx) {
  BUSTUB_ASSERT(idx >= 0 && idx < GetSize(), "index out of range");
  int size{GetSize()};
  for (int i = idx; i < size - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    page_id_array_[i] = page_id_array_[i + 1];
  }

  SetSize(size - 1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
