//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.cpp
//
// Identification: src/primer/hyperloglog.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), leading_b_{n_bits} {
  uint64_t m = std::pow(2, n_bits);
  hll_register_.resize(m, 0);
}

/**
 * @brief Function that computes binary.
 *
 * @param[in] hash
 * @returns binary of a given hash
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  /** @TODO(student) Implement this function! */
  return std::bitset<BITSET_CAPACITY>{hash};
}

/**
 * @brief Function that computes leading zeros.
 *
 * @param[in] bset - binary values of a given bitset
 * @returns leading zeros of given binary set
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  int startIndex = (BITSET_CAPACITY - 1);
  uint64_t posOne = 1;

  for (; startIndex >= 0; startIndex--) {
    if (bset[startIndex] == 1) {
      return posOne;
    }
    posOne++;
  }
  return 0;
}

/**
 * @brief Adds a value into the HyperLogLog.
 *
 * @param[in] val - value that's added into hyperloglog
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  std::lock_guard lk{mtx};
  hasInserted = true;
  hash_t hashVal = CalculateHash(val);

  uint64_t b = 0;
  // CRITICAL FIX: Avoid shifting by 64 bits if leading_b_ is 0 (Undefined Behavior)
  if (leading_b_ > 0) {
    b = hashVal >> (BITSET_CAPACITY - leading_b_);
  }
  uint64_t r = (hashVal << leading_b_);

  if (b >= pow(2, leading_b_)) return;

  hll_register_[b] = std::max(hll_register_[b], PositionOfLeftmostOne(std::bitset<BITSET_CAPACITY>(r)));
}

/**
 * @brief Function that computes cardinality.
 */
template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (!hasInserted) return;

  uint64_t m = pow(2, leading_b_);
  double sum = 0;

  for (auto reg_val : hll_register_) {
    sum += std::pow(2.0, -static_cast<double>(reg_val));
  }
  cardinality_ = floor((CONSTANT * std::pow(m, 2)) / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
