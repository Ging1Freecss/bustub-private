//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <cstddef>
#include <queue>
#include "common/config.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Construct a new TopNExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The TopN plan to be executed
 */
TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the TopN */
void TopNExecutor::Init() {
  child_executor_->Init();
  result_.clear();
  idx_ = 0;
  is_finished_ = false;

  TupleComparator cmp{plan_->GetOrderBy()};
  auto comparator{[&cmp](const SortEntry &a, const SortEntry &b) -> bool { return cmp(a, b); }};
  std::priority_queue<SortEntry, std::vector<SortEntry>, decltype(comparator)> pq{comparator};

  size_t N{plan_->GetN()};

  std::vector<Tuple> child_tuple{};
  std::vector<RID> child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid, BUSTUB_BATCH_SIZE)) {
    for (size_t i{0}; i < child_tuple.size(); i++) {
      SortEntry a{GenerateSortKey(child_tuple[i], plan_->order_bys_, *plan_->output_schema_), child_tuple[i]};
      if (pq.size() < N) {
        pq.push(std::move(a));
      } else if (pq.size() == N && cmp(a, pq.top())) {
        pq.pop();
        pq.push(std::move(a));
      }
    }
  }

  while (!pq.empty()) {
    result_.push_back(std::move(pq.top().second));
    pq.pop();
  }
  total_tuples = result_.size();
  std::reverse(result_.begin(), result_.end());
}

/**
 * Yield the next tuple batch from the TopN.
 * @param[out] tuple_batch The next tuple batch produced by the TopN
 * @param[out] rid_batch The next tuple RID batch produced by the TopN
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto TopNExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
    -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (idx_ < result_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(std::move(result_[idx_]));
    rid_batch->push_back(RID{});
    idx_++;
    total_tuples--;
  }

  is_finished_ = tuple_batch->empty();
  return !is_finished_;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return total_tuples; }

}  // namespace bustub
