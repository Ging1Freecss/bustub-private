//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <queue>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinExecutor executes index join operations.
 */
class NestedIndexJoinExecutor : public AbstractExecutor {
 public:
  NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                          std::unique_ptr<AbstractExecutor> &&child_executor);

  /** @return The output schema for the nested index join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_{nullptr};

  std::shared_ptr<TableInfo> table_info_{nullptr};
  std::shared_ptr<IndexInfo> index_info_{nullptr};

  // left tuple batch
  std::vector<Tuple> child_tuple_batch_{};
  std::size_t child_idx_{0};
  std::vector<bustub::RID> rid_batch_child{};

  // tuple of left match with right
  // < tuple of left tuple , tuple of right table>
  std::vector<std::pair<std::pair<Tuple, Tuple>, bool>> match_tuple_{};
  std::size_t match_tuple_idx_{0};

  // no left tuple to match with
  bool is_finished{false};
  bool child_exist{false};
};
}  // namespace bustub
