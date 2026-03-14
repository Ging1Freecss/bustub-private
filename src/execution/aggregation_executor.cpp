//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <memory>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  std::vector<bustub::Tuple> tuple_batch_child{};
  std::vector<bustub::RID> rid_batch_child{};

  while (child_executor_->Next(&tuple_batch_child, &rid_batch_child, BUSTUB_BATCH_SIZE)) {
    for (std::size_t i{0}; i < tuple_batch_child.size(); i++) {
      auto agg_key = MakeAggregateKey(&tuple_batch_child[i]);
      auto agg_value = MakeAggregateValue(&tuple_batch_child[i]);

      aht_.InsertCombine(agg_key, agg_value);
    }
    tuple_batch_child.clear();
    rid_batch_child.clear();
  }

  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {  // empty case
    AggregateKey agg_key{};
    // this is important when a table is empty and countstar is the query then
    // CombineAggregateValues which is used in InsertCombine will add 1, so output is 1 instead of 0
    // thats why i created this insert  empty val
    // else other type of query should just return null anyway
    aht_.InsertInitial(agg_key);
  }

  aht_iterator_ = aht_.Begin();
}

/**
 * Yield the next tuple batch from the aggregation.
 * @param[out] tuple_batch The next batch of tuples produced by the aggregation
 * @param[out] rid_batch The next batch of tuple RIDs produced by the aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if any tuples were produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  while (aht_iterator_ != aht_.End() && tuple_batch->size() < batch_size) {
    std::vector<Value> values;

    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());

    Tuple tuple{values, &GetOutputSchema()};
    tuple_batch->emplace_back(tuple);
    rid_batch->emplace_back(RID{});

    ++aht_iterator_;
  }
  return !tuple_batch->empty();
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
