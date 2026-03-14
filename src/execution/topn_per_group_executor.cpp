//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_per_group_executor.cpp
//
// Identification: src/execution/topn_per_group_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_per_group_executor.h"
#include <cstddef>
#include <map>
#include <tuple>
#include <unordered_map>
#include <utility>
#include "common/config.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new TopNPerGroupExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The TopNPerGroup plan to be executed
 */
TopNPerGroupExecutor::TopNPerGroupExecutor(ExecutorContext *exec_ctx, const TopNPerGroupPlanNode *plan,
                                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the TopNPerGroup */
void TopNPerGroupExecutor::Init() {
  child_executor_->Init();
  result_.clear();
  idx_ = 0;
  is_finished_ = false;

  size_t N{plan_->GetN()};

  TupleComparator cmp{plan_->GetOrderBy()};

  using map_type = std::map<SortEntry, std::vector<Tuple>, decltype(cmp)>;

  struct Partition_Data {
    map_type score_groups_;
    size_t count_{0};

    explicit Partition_Data(const TupleComparator &cmp) : score_groups_(cmp) {}
  };
  std::unordered_map<partition_key, Partition_Data> hash_map_partition;

  std::vector<Tuple> child_tuple;
  std::vector<RID> child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid, BUSTUB_BATCH_SIZE)) {
    for (size_t i{0}; i < child_tuple.size(); i++) {
      partition_key p_key;
      p_key.values.reserve(plan_->group_bys_.size());

      for (auto &expr : plan_->group_bys_) {
        p_key.values.push_back(expr->Evaluate(&child_tuple[i], child_executor_->GetOutputSchema()));
      }

      SortKey s_key{GenerateSortKey(child_tuple[i], plan_->GetOrderBy(), child_executor_->GetOutputSchema())};
      SortEntry s_entry{s_key, child_tuple[i]};

      auto it = hash_map_partition.find(p_key);

      if (it == hash_map_partition.end()) {
        it = hash_map_partition
                 .emplace(std::piecewise_construct, std::forward_as_tuple(p_key), std::forward_as_tuple(cmp))
                 .first;
      }

      auto &data = it->second;
      data.score_groups_[s_entry].push_back(std::move(child_tuple[i]));
      data.count_++;

      // 1.a|2.b|3.c|4.c|5.c -> position
      // 1,2,3,3,3 the way rank is calc, if ele are same then rank = position of same ele at first position
      // since here we insert element without considering rank
      // out goal is to remove element with rank n+1
      // so if total_count(i.e data.count here) - worst group size(size of vector of with SortEntry it is the last
      // element in map ) = rank of the element
      while (!data.score_groups_.empty()) {
        auto last_it = std::prev(data.score_groups_.end());
        size_t worst_group_size = last_it->second.size();

        size_t rank = data.count_ - worst_group_size + 1;

        if (rank > N) {
          data.count_ -= worst_group_size;
          data.score_groups_.erase(last_it);
        } else {
          break;
        }
      }
    }
  }

  for (auto &pair : hash_map_partition) {
    size_t position = 1;
    for (auto &group : pair.second.score_groups_) {
      size_t current_rank = position;
      for (auto &tuple : group.second) {
        std::vector<Value> values;
        values.reserve(plan_->OutputSchema().GetColumnCount());

        // 1. Copy original columns from child
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }

        // 2. Append the Rank column
        values.push_back(ValueFactory::GetIntegerValue(current_rank));

        // 3. Construct the new enriched tuple
        result_.emplace_back(std::move(values), &plan_->OutputSchema());
      }
      // Increment position by the number of tuples sharing this rank
      position += group.second.size();
    }
  }
  total_tuples = result_.size();
}

/**
 * Yield the next tuple batch from the TopNPerGroup.
 * @param[out] tuple_batch The next tuple batch produced by the TopNPerGroup
 * @param[out] rid_batch The next tuple RID batch produced by the TopNPerGroup
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto TopNPerGroupExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                size_t batch_size) -> bool {
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

}  // namespace bustub
