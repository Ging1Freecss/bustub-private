//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }
  void InsertTuple(const Tuple &tuple) {
    if (pages_.empty()) {
      page_id_t page_id{bpm_->NewPage()};
      std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(page_id, AccessType::Lookup)};

      if (!page_gaurd.has_value()) {
        throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
      }
      pages_.push_back(page_id);
      IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
      page_data->Init();
      page_data->InsertTuple(tuple);

      num_tuples_++;
      return;
    }

    // for condition when tuple was inserted into the last page of partition
    {
      page_id_t last_page_id{pages_.back()};
      std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(last_page_id, AccessType::Lookup)};
      if (!page_gaurd.has_value()) {
        throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
      }
      IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
      bool was_insert = page_data->InsertTuple(tuple);

      if (was_insert) {  // page has space
        num_tuples_++;
        return;
      }
    }

    // if not means last page was full
    page_id_t page_id{bpm_->NewPage()};
    std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(page_id, AccessType::Lookup)};
    if (!page_gaurd.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }
    pages_.push_back(page_id);
    IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
    page_data->Init();
    page_data->InsertTuple(tuple);

    num_tuples_++;
    return;
  }

  void Clear() {
    for (page_id_t i : pages_) {
      bpm_->DeletePage(i);
    }
    pages_.clear();
    num_tuples_ = 0;
  }
  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      if (idx_ == run_->pages_.size() || !tuple_.has_value()) {
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
      } else if (slot_num_ == max_slot_num) {
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
        throw bustub::Exception("error when accessing value of iterator, tuple_ = std::nullopt");
      }
      return tuple_.value();
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      if (!tuple_.has_value() && !other.tuple_.has_value()) return true;
      return idx_ == other.idx_ && slot_num_ == other.slot_num_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !operator==(other); }

   private:
    explicit Iterator(const MergeSortRun *run, std::size_t idx) : run_(run), idx_(idx), slot_num_(0) {
      if (run_->pages_.size() > idx_) {
        page_id_t page_id{run_->pages_[idx_]};

        page_guard_ = run_->bpm_->CheckedReadPage(page_id);

        if (!page_guard_.has_value()) {
          throw bustub::Exception("buffer pool manager failed");
        }

        const IntermediateResultPage *page_ptr{page_guard_->As<IntermediateResultPage>()};
        uint32_t max_slot_num{page_ptr->GetNumTuples()};

        if (slot_num_ < max_slot_num) {
          tuple_ = page_ptr->GetTuple(slot_num_);
        }
      }
    }

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_;

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

 private:
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
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  void Initialise_Pages();
  void MergeEveryThing();

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO(P3): You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_{nullptr};
  MergeSortRun run_;
  MergeSortRun::Iterator current_;
  bool is_finished{false};
  std::vector<MergeSortRun> run_vec{};
};

}  // namespace bustub
