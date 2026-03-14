//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cmp_(plan->GetOrderBy()),
      child_executor_(std::move(child_executor)),
      run_(std::vector<page_id_t>{}, exec_ctx->GetBufferPoolManager()) {}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  is_finished = false;
  Initialise_Pages();
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Initialise_Pages() {
  std::vector<Tuple> child_tuple{};
  std::vector<RID> child_rid{};

  std::vector<SortEntry> buffer_{};

  BufferPoolManager *bpm_{exec_ctx_->GetBufferPoolManager()};

  page_id_t new_page_id{bpm_->NewPage()};
  std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(new_page_id)};
  IntermediateResultPage *page_ptr{page_gaurd->AsMut<IntermediateResultPage>()};
  page_ptr->Init();

  auto refresh_page{[&new_page_id, &page_gaurd, &page_ptr, &bpm_]() {
    new_page_id = bpm_->NewPage();
    page_gaurd = bpm_->CheckedWritePage(new_page_id);
    page_ptr = page_gaurd->AsMut<IntermediateResultPage>();
    page_ptr->Init();
  }};

  auto delete_page{[&new_page_id, &page_gaurd, &bpm_] {
    page_gaurd.reset();
    bpm_->DeletePage(new_page_id);
  }};

  while (child_executor_->Next(&child_tuple, &child_rid, BUSTUB_BATCH_SIZE)) {
    for (size_t i{0}; i < child_tuple.size();) {
      if (!page_ptr->InsertTuple(child_tuple[i])) {
        uint32_t max_num_tp{page_ptr->GetNumTuples()};

        for (uint32_t a{0}; a < max_num_tp; a++) {
          Tuple tp{page_ptr->GetTuple(a)};
          SortKey key{GenerateSortKey(tp, plan_->order_bys_, child_executor_->GetOutputSchema())};
          buffer_.push_back({std::move(key), tp});
        }

        std::sort(buffer_.begin(), buffer_.end(), cmp_);
        delete_page();
        refresh_page();
        for (size_t a{0}; a < buffer_.size(); a++) {
          page_ptr->InsertTuple(buffer_[a].second);
        }
        run_vec.emplace_back(std::vector{new_page_id}, bpm_);
        buffer_.clear();
        refresh_page();
        continue;
      }
      i++;
    }
  }

  if (page_ptr->GetNumTuples() > 0) {
    uint32_t max_num_tp{page_ptr->GetNumTuples()};

    for (uint32_t a{0}; a < max_num_tp; a++) {
      Tuple tp{page_ptr->GetTuple(a)};
      SortKey key{GenerateSortKey(tp, plan_->order_bys_, child_executor_->GetOutputSchema())};
      buffer_.push_back({std::move(key), tp});
    }

    std::sort(buffer_.begin(), buffer_.end(), cmp_);
    delete_page();
    refresh_page();
    for (size_t a{0}; a < buffer_.size(); a++) {
      page_ptr->InsertTuple(buffer_[a].second);
    }
    run_vec.emplace_back(std::vector{new_page_id}, bpm_);
    buffer_.clear();
  }

  page_gaurd.reset();
  MergeEveryThing();
}

template <size_t K>
void ExternalMergeSortExecutor<K>::MergeEveryThing() {
  auto MergeTwo{[&](MergeSortRun &m1, MergeSortRun &m2, MergeSortRun &temp_buf) {
    MergeSortRun::Iterator a = m1.Begin();
    MergeSortRun::Iterator b = m2.Begin();
    if (a == m1.End() && b == m2.End()) {
      return;
    }

    SortKey key_a, key_b;
    bool has_a = false;
    bool has_b = false;

    if (a != m1.End()) {
      key_a = GenerateSortKey(*a, plan_->order_bys_, child_executor_->GetOutputSchema());
      has_a = true;
    }
    if (b != m2.End()) {
      key_b = GenerateSortKey(*b, plan_->order_bys_, child_executor_->GetOutputSchema());
      has_b = true;
    }
    TupleComparator cmp{plan_->order_bys_};
    while (has_a && has_b) {
      SortEntry entry_a{key_a, *a};
      SortEntry entry_b{key_b, *b};
      if (cmp(entry_a, entry_b)) {
        temp_buf.InsertTuple(*a);
        ++a;
        if (a != m1.End()) {
          key_a = GenerateSortKey(*a, plan_->order_bys_, child_executor_->GetOutputSchema());
        } else {
          has_a = false;
        }
      } else {
        temp_buf.InsertTuple(*b);
        ++b;
        if (b != m2.End()) {
          key_b = GenerateSortKey(*b, plan_->order_bys_, child_executor_->GetOutputSchema());
        } else {
          has_b = false;
        }
      }
    }

    while (a != m1.End()) {
      temp_buf.InsertTuple(*a);
      ++a;
    }

    while (b != m2.End()) {
      temp_buf.InsertTuple(*b);
      ++b;
    }
  }};

  while (run_vec.size() > 1) {
    std::vector<MergeSortRun> new_runs{};
    for (size_t i{0}; i < run_vec.size(); i += 2) {
      if (i + 1 >= run_vec.size()) {
        // Odd run, pass through
        new_runs.push_back(std::move(run_vec[i]));
        continue;
      }
      MergeSortRun temp_buf{std::vector<page_id_t>{}, exec_ctx_->GetBufferPoolManager()};
      MergeTwo(run_vec[i], run_vec[i + 1], temp_buf);

      run_vec[i].Clear();
      run_vec[i + 1].Clear();

      new_runs.push_back(std::move(temp_buf));
    }
    run_vec = std::move(new_runs);
  }

  if (run_vec.size() == 1) {
    run_ = std::move(run_vec[0]);
  }
  current_ = run_.Begin();
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge sort.
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (is_finished) return false;
  while (current_ != run_.End() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(*current_);
    rid_batch->push_back(RID{});
    // std::cerr << "hello";
    ++current_;
  }

  is_finished = tuple_batch->empty();

  if (is_finished) {
    run_.Clear();
  }

  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
