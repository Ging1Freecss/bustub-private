//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog_presto.cpp
//
// Identification: src/primer/hyperloglog_presto.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/hyperloglog_presto.h"

namespace bustub {

/** @brief Parameterized constructor. */
template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits)
    : cardinality_(0), n_leading_bits_{n_leading_bits} {

  uint64_t m = std::pow(2, static_cast<uint64_t>(n_leading_bits_));
  dense_bucket_.resize(m, std::bitset<DENSE_BUCKET_SIZE>{0});
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::PositionOfRightmostOne(bustub::hash_t hash_value) -> uint8_t {

  std::bitset<64> b{hash_value};
  uint8_t pos{0};
  
  for (size_t i = 0; i < 64; i++) {
    if (b[i] == 1) return pos;
    pos++;
  }
  return 64;
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::getWholeNumber(int64_t idx) -> uint8_t {
  
  uint8_t lsb = static_cast<uint8_t>(dense_bucket_[idx].to_ullong());
  
  uint64_t m_msb = overflow_bucket_.find(idx) == overflow_bucket_.end() ? 0 : overflow_bucket_.at(idx).to_ullong();
  uint8_t msb = static_cast<uint8_t>(m_msb) << 4;

  return (lsb | msb);
}

/** @brief Element is added for HLL calculation. */
template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hasStarted = true;
  bustub::hash_t hash_value = CalculateHash(val);
  uint64_t b{0};

  if (n_leading_bits_ > 0) {
    b = hash_value >> (64 - n_leading_bits_);
  }

  uint8_t pos = PositionOfRightmostOne(hash_value);
  if (pos >= (64 - n_leading_bits_)) { // important as pos value at max can be equal to available bits after using bits for b
    pos = 64 - n_leading_bits_;
  }

  uint8_t msb = (static_cast<int>(pos) & 0x70) >> 4;
  uint8_t lsb = pos & 0x0F;

  if (pos > getWholeNumber(b)) {
    
    dense_bucket_[b] = std::bitset<DENSE_BUCKET_SIZE>{lsb};  
    overflow_bucket_[static_cast<uint16_t>(b)] = std::bitset<OVERFLOW_BUCKET_SIZE>{msb};
  }

}

/** @brief Function to compute cardinality. */
template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if(!hasStarted) return;

  uint64_t m = std::pow(2, n_leading_bits_);
  double sum{0.0};

  for (uint64_t i = 0; i < m; i++) {
    uint8_t w = getWholeNumber(i);
    sum += std::pow(2.0, -static_cast<double>(w));
  }

  cardinality_ = floor((CONSTANT * std::pow(static_cast<double>(m), 2)) / sum);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
