//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(P4): implement me!
  current_reads_[read_ts]++;
  watermark_ = current_reads_.begin()->first;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(P4): implement me!
  auto it = current_reads_.find(read_ts);

  if (it == current_reads_.end()) {
    throw Exception("read_ts does not exist in current_reads_");
  }
  it->second--;

  if (it->second == 0) {
    current_reads_.erase(it);
  }

  if (current_reads_.empty()) {
    watermark_ = commit_ts_;
  } else {
    watermark_ = current_reads_.begin()->first;
  }
}

}  // namespace bustub
