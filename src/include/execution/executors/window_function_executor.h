//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "binder/bound_order_by.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "fmt/base.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

class TupleDiskStorage {
 public:
  TupleDiskStorage() = default;
  TupleDiskStorage(std::vector<page_id_t> pages, BufferPoolManager *bpm, size_t num_tuples = 0)
      : pages_(std::move(pages)), bpm_(bpm), num_tuples_(num_tuples) {}

  auto GetPageCount() -> size_t { return pages_.size(); }

  // insert tuple to appropriate page in std::vector<page_id_t> pages_;
  void InsertTuple(const Tuple &tuple) {
    if (pages_.empty()) {
      page_id_t page_id{bpm_->NewPage()};
      std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(page_id)};

      if (!page_gaurd.has_value()) {
        throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
      }
      IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
      // fmt::print(stderr, "DEBUG: Run {} NewPage {} for tuple 1\n", fmt::ptr(this), page_id);
      page_data->Init();
      if (page_data->InsertTuple(tuple)) {
        pages_.push_back(page_id);
        num_tuples_++;
        return;
      }
      // fmt::print(stderr, "DEBUG: Run {} deleting page {} (InsertTuple failed)\n", fmt::ptr(this), page_id);
      bpm_->DeletePage(page_id);
      throw bustub::ExecutionException("Tuple too large for empty page");
    }

    // for condition when tuple was inserted into the last page of partition
    {
      page_id_t last_page_id{pages_.back()};
      std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(last_page_id)};
      if (!page_gaurd.has_value()) {
        throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
      }
      IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
      bool was_insert = page_data->InsertTuple(tuple);

      if (was_insert) {  // page has space
        num_tuples_++;
        return;
      }
      page_gaurd.reset();
      bpm_->FlushPage(last_page_id);
    }

    // if not means last page was full
    page_id_t page_id{bpm_->NewPage()};
    if (page_id == INVALID_PAGE_ID) throw bustub::Exception("BPM OOM");
    std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(page_id)};

    if (!page_gaurd.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }
    IntermediateResultPage *page_data = page_gaurd->AsMut<IntermediateResultPage>();
    // fmt::print(stderr, "DEBUG: Run {} NewPage {} (last page full)\n", fmt::ptr(this), page_id);
    page_data->Init();
    if (page_data->InsertTuple(tuple)) {
      pages_.push_back(page_id);
      num_tuples_++;
      if (page_data->GetNumTuples() == 0) {
        fmt::print(stderr, "CRITICAL: Page {} reports 0 tuples physically after successful InsertTuple!\n", page_id);
      }
    } else {
      page_gaurd.reset();
      // fmt::print(stderr, "DEBUG: Run {} deleting page {} (tuple too large)\n", fmt::ptr(this), page_id);
      bpm_->DeletePage(page_id);
      throw bustub::ExecutionException("Tuple too large for empty page");
    }

    if (num_tuples_ % 1000000 == 0) {
      // fmt::print("DEBUG: TupleDiskStorage::InsertTuple num_tuples_={}\n", num_tuples_);
    }
    return;
  }

  // delete the content from all pages in std::vector pages_
  void Clear() {
    for (page_id_t i : pages_) {
      // fmt::print(stderr, "DEBUG: Run {} Clear: Deleting Page {}\n", fmt::ptr(this), i);
      if (!bpm_->DeletePage(i)) {
        //  auto pin_count = bpm_->GetPinCount(i);
        // fmt::print(stderr, "WARNING: Run {} DeletePage({}) failed! Pin Count: {}\n", fmt::ptr(this), i,
        //            pin_count ? *pin_count : -1);
      }
    }
    pages_.clear();
    num_tuples_ = 0;
  }

  void Flush() {
    for (page_id_t i : pages_) {
      bpm_->FlushPage(i);
    }
  }

  auto GetNumTuples() const -> size_t { return num_tuples_; }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class TupleDiskStorage;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      if (idx_ >= run_->pages_.size() || !tuple_.has_value()) {
        tuple_ = std::nullopt;
        slot_num_ = 0;
        page_guard_.reset();
        idx_ = run_->pages_.size();
        return *this;
      }

      const IntermediateResultPage *page_ptr{page_guard_->As<IntermediateResultPage>()};

      uint32_t max_slot_num{page_ptr->GetNumTuples()};
      slot_num_++;

      if (slot_num_ < max_slot_num) {
        tuple_ = page_ptr->GetTuple(slot_num_);
      } else if (slot_num_ >= max_slot_num) {
        page_guard_.reset();
        idx_++;         // next page
        slot_num_ = 0;  // start of tuple in next page

        // reach end page vector
        if (idx_ == run_->pages_.size()) {
          tuple_.reset();
          return *this;
        }

        page_guard_ = run_->bpm_->CheckedReadPage(run_->pages_[idx_]);

        if (!page_guard_.has_value()) {
          throw bustub::Exception("buffer pool manager failed");
        }

        page_ptr = page_guard_->As<IntermediateResultPage>();

        if (page_ptr->GetNumTuples() == 0) {
          fmt::print(stderr, "DEBUG: no tuples at IntermediateResultPage -> page_id={} run_size={} idx_={}\n",
                     run_->pages_[idx_], run_->pages_.size(), idx_);
          throw bustub::Exception(" no tuples at IntermediateResultPage ");
        }
        tuple_ = page_ptr->GetTuple(slot_num_);
      }
      return *this;
    }
    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() -> Tuple {
      if (!tuple_.has_value()) {
        const IntermediateResultPage *page_ptr{page_guard_->As<IntermediateResultPage>()};
        fmt::println("num tuples: {}", page_ptr->GetNumTuples());
        fmt::println("page id: {}", page_guard_->GetPageId());
        throw bustub::Exception("error when accessing value of iterator, tuple_ = std::nullopt");
      }
      return tuple_.value();
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      // if (!tuple_.has_value() && !other.tuple_.has_value()) return true;
      return idx_ == other.idx_ && slot_num_ == other.slot_num_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !operator==(other); }

   public:
    explicit Iterator(const TupleDiskStorage *run, std::size_t idx) : run_(run), idx_(idx), slot_num_(0) {
      if (run_->pages_.size() > idx_) {
        page_id_t page_id{run_->pages_[idx_]};

        page_guard_ = run_->bpm_->CheckedReadPage(page_id);

        if (!page_guard_.has_value()) {
          throw bustub::Exception("buffer pool manager failed");
        }

        const IntermediateResultPage *page_ptr{page_guard_->As<IntermediateResultPage>()};
        uint32_t max_slot_num{page_ptr->GetNumTuples()};

        if (page_ptr->GetNumTuples() == 0) {
          const char *data = page_guard_->GetData();
          fmt::print(stderr,
                     "ERROR: EMPTY PAGE {} in run {} (idx {}). num_tuples_ (obj)={}. Raw Header (Hex): {:02x} {:02x} "
                     "{:02x} {:02x}\n",
                     page_id, fmt::ptr(run_), idx_, run_->num_tuples_, (uint8_t)data[0], (uint8_t)data[1],
                     (uint8_t)data[2], (uint8_t)data[3]);
          throw bustub::Exception("EMPTY PAGE DETECTED IN TupleDiskStorage");
        }

        if (slot_num_ < max_slot_num) {
          tuple_ = page_ptr->GetTuple(slot_num_);
        }
      }
    }

    /** The sorted run that the iterator is iterating on. */
    const TupleDiskStorage *run_;

    /**
     * TODO(P3): Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    // index of std::vector<page_id_t> pages_;
    std::size_t idx_{0};
    // next slot number of tuple in page
    uint32_t slot_num_{0};
    // current tuple pointed by iterator
    std::optional<Tuple> tuple_{std::nullopt};
    std::optional<ReadPageGuard> page_guard_{std::nullopt};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() -> Iterator { return Iterator{this, 0}; }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() -> Iterator { return Iterator{this, pages_.size()}; }

 public:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
  // total tuples
  size_t num_tuples_{0};
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputting rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functions and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  void initialise_input();
  auto process_window_functions(uint32_t wind_fn_idx) -> void;
  auto do_mergePass(std::vector<TupleDiskStorage> &runs, TupleComparator &cmp, std::vector<OrderBy> &combine_keys,
                    Schema &tuple_schema) -> TupleDiskStorage;

  auto calc_window_function(TupleDiskStorage &input_for_wind, TupleComparator &cmp, std::vector<OrderBy> &combine_keys,
                            Schema &tuple_schema, const WindowFunctionPlanNode::WindowFunction &wf)
      -> std::tuple<TupleDiskStorage, std::vector<OrderBy>, Schema>;

  auto standard_tuple_disk_storage_sort(TupleDiskStorage &page_input, TupleComparator &cmp,
                                        std::vector<OrderBy> &combine_keys, Schema &tuple_schema) -> TupleDiskStorage;
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // std::vector<TupleDiskStorage> run_vec{};
  // TupleComparator cmp_;
  std::unordered_map<uint32_t, std::vector<TupleDiskStorage>> tuples_per_window{};
  std::unordered_map<uint32_t, std::tuple<TupleDiskStorage, Schema, std::vector<OrderBy>>> calc_output{};
  TupleDiskStorage output_tuples{};
  TupleDiskStorage::Iterator output_iter_;

  std::optional<Schema> enriched_schema{std::nullopt};
  std::vector<std::unordered_map<uint32_t, Value>> results{};
  bool is_finished{false};
  size_t total_tuples{0};
  std::vector<OrderBy> output_order;

  // take input from child executor and a extra element=row number
  TupleDiskStorage input_tuples{};

  TupleDiskStorage::Iterator input_iter_;
  std::unordered_map<uint32_t, TupleDiskStorage::Iterator> window_iters_;
};
}  // namespace bustub
