#pragma once

#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
 public:
  /**
   * TODO(P3): Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  void Init();
  auto InsertTuple(const Tuple &tuple) -> bool;
  auto GetTuple(uint32_t idx) const -> Tuple;
  auto GetNumTuples() const -> uint32_t;
  auto IsFull(const Tuple &tuple) const -> bool;
  auto GetNextTupleOffset(const Tuple &tuple) const -> std::optional<uint16_t>;

 private:
  /**
   * TODO(P3): Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
 private:
  /**
   * Layout:
   *
   * +----------------------+
   * | num_tuples_          |
   * +----------------------+
   * | TupleSlot array      |
   * | slots[0]             |
   * | slots[1]             |
   * | ...                  |
   * +----------------------+
   * | free space           |
   * |                      |
   * +----------------------+
   * | tuple data           |
   * | tuple data           |
   * +----------------------+
   */

  struct TupleSlot {
    uint16_t offset_;
    uint16_t size_;
  };

  /** number of tuples stored in page */
  uint32_t num_tuples_{0};

  /** return pointer to page start */
  auto GetPageStart() -> char * { return reinterpret_cast<char *>(this); }

  auto GetPageStart() const -> const char * { return reinterpret_cast<const char *>(this); }

  /** return slot array start */
  auto GetSlots() -> TupleSlot * {
    return reinterpret_cast<TupleSlot *>(reinterpret_cast<char *>(this) + sizeof(num_tuples_));
  }

  auto GetSlots() const -> const TupleSlot * {
    return reinterpret_cast<const TupleSlot *>(reinterpret_cast<const char *>(this) + sizeof(num_tuples_));
  }
};

}  // namespace bustub
