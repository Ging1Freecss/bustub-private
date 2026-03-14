#include "storage/page/intermediate_result_page.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {
void IntermediateResultPage::Init() {
  num_tuples_ = 0;
  // LOGGING: fmt::print(stderr, "DEBUG: IntermediateResultPage::Init() called on Page ID unknown\n");
  // Unfortunately this class doesn't store its own PageID easily without a pointer.
  // We'll rely on the caller log.
}

auto IntermediateResultPage::GetNumTuples() const -> uint32_t { return num_tuples_; }

auto IntermediateResultPage::GetNextTupleOffset(const Tuple &tuple) const -> std::optional<uint16_t> {
  const size_t tuple_size{tuple.GetLength()};
  const size_t header_size{sizeof(num_tuples_) + sizeof(TupleSlot) * (num_tuples_ + 1)};

  // 1. Header itself must not exceed page size
  if (header_size > BUSTUB_PAGE_SIZE) {
    return std::nullopt;
  }
  const auto *slots = GetSlots();
  size_t prev_offset_pos{};
  if (num_tuples_ > 0) {
    prev_offset_pos = slots[num_tuples_ - 1].offset_;
  } else {
    prev_offset_pos = BUSTUB_PAGE_SIZE;
  }

  // 2. Ensure prev offset was sane and prevent underflow
  if (prev_offset_pos > BUSTUB_PAGE_SIZE || prev_offset_pos < tuple_size) {
    return std::nullopt;
  }

  size_t curr_offset_pos = prev_offset_pos - tuple_size;
  curr_offset_pos = curr_offset_pos - (curr_offset_pos % 8);
  // 3. Tuple must start after the header (Collision Check)
  if (curr_offset_pos < header_size) {
    return std::nullopt;
  }

  return static_cast<uint16_t>(curr_offset_pos);
}

auto IntermediateResultPage::IsFull(const Tuple &tuple) const -> bool { return !GetNextTupleOffset(tuple).has_value(); }

auto IntermediateResultPage::InsertTuple(const Tuple &tuple) -> bool {
  std::optional<uint16_t> curr_offset_pos{GetNextTupleOffset(tuple)};

  if (!curr_offset_pos.has_value()) return false;

  uint32_t tuple_idx{num_tuples_};
  auto *slots = GetSlots();
  auto *page_start = GetPageStart();
  uint16_t offset = curr_offset_pos.value();

  slots[tuple_idx] = TupleSlot{.offset_ = static_cast<uint16_t>(curr_offset_pos.value()),
                               .size_ = static_cast<uint16_t>(tuple.GetLength())};
  num_tuples_++;
  std::memcpy(page_start + offset, tuple.GetData(), tuple.GetLength());
  return true;
}

auto IntermediateResultPage::GetTuple(uint32_t tuple_id) const -> Tuple {
  if (tuple_id >= num_tuples_) {
    throw bustub::Exception("invalid tuple access in intermediate_result_page.cpp GetTuple tuple_id >= num_tuples_");
  }

  const auto *slots = GetSlots();
  const auto *page_start = GetPageStart();

  struct TupleSlot tp {
    slots[tuple_id]
  };

  Tuple tuple;
  tuple.data_.resize(tp.size_);

  std::memmove(tuple.data_.data(), page_start + tp.offset_, tp.size_);
  tuple.rid_ = RID{tuple_id};

  return tuple;
}
}  // namespace bustub