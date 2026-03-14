//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Construct a new LimitExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The limit plan to be executed
 * @param child_executor The child executor from which limited tuples are pulled
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the limit */
void LimitExecutor::Init() {
  child_executor_->Init();
  num_tuples = 0;
  is_finished = false;
}

/**
 * Yield the next tuple batch from the limit.
 * @param[out] tuple_batch The next tuple batch produced by the limit
 * @param[out] rid_batch The next tuple RID batch produced by the limit
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto LimitExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                         size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (is_finished) return false;

  std::vector<Tuple> child_tuple{};
  std::vector<RID> child_rid{};

  while (child_executor_->Next(&child_tuple, &child_rid, batch_size)) {
    if (num_tuples + child_tuple.size() <= plan_->GetLimit() && child_tuple.size() == batch_size) {
      num_tuples += child_tuple.size();
      (*tuple_batch) = std::move(child_tuple);
      (*rid_batch) = std::move(child_rid);
      break;

    } else {
      for (size_t i{0}; i < child_tuple.size(); i++) {
        if (num_tuples + 1 <= plan_->GetLimit()) {
          tuple_batch->push_back(child_tuple[i]);
          rid_batch->push_back(child_rid[i]);
          num_tuples++;
        } else {
          is_finished = true;
          return !tuple_batch->empty();
        }
      }
    }
    child_tuple.clear();
    child_rid.clear();
  }
  is_finished = (num_tuples == plan_->GetLimit()) || tuple_batch->empty();
  return !tuple_batch->empty();
}

}  // namespace bustub
