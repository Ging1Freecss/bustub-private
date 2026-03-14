//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;

  if (plan_->predicate_ == nullptr) {
    throw bustub::NotImplementedException(fmt::format("no ON clause"));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_idx_ = 0;
  right_idx_ = 0;
  tuple_batch_left.clear();
  rid_batch_left.clear();
  tuple_batch_right.clear();
  rid_batch_right.clear();

  left_matched_.assign(BUSTUB_BATCH_SIZE, false);
  left_matched_arr_.clear();
  left_matched_idx_ = 0;

  left_ele_exist_ = left_executor_->Next(&tuple_batch_left, &rid_batch_left, BUSTUB_BATCH_SIZE);
  right_ele_exist_ = right_executor_->Next(&tuple_batch_right, &rid_batch_right, BUSTUB_BATCH_SIZE);
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  // drain left table and extracted the (left,null) tuples

  auto cleanUp_left_matched{[tuple_batch, rid_batch, batch_size, this]() -> void {
    if (tuple_batch->size() < batch_size) {
      size_t i{left_matched_idx_};

      for (; (i < left_matched_arr_.size()) && (tuple_batch->size() < batch_size); i++) {
        tuple_batch->push_back(left_matched_arr_[i]);
        rid_batch->push_back(RID{});
      }
      left_matched_idx_ = i;

      if (left_matched_idx_ >= left_matched_arr_.size()) {
        // clear memory
        left_matched_arr_.clear();
        left_matched_idx_ = 0;
      }
    }
  }};

  if (plan_->GetJoinType() == JoinType::LEFT) {
    if (!left_ele_exist_ && left_matched_idx_ >= left_matched_arr_.size()) {
      // clear memory
      left_matched_arr_.clear();
      left_matched_idx_ = 0;
      return false;
    }
    cleanUp_left_matched();
  } else {
    // inner join left table exhausted
    if (!left_ele_exist_) {
      return false;
    }
  }
  // initial data
  const Schema &left_schema = left_executor_->GetOutputSchema();
  const Schema &right_schema = right_executor_->GetOutputSchema();

  while (tuple_batch->size() < batch_size && left_ele_exist_) {
    // match left tuple batch with right tuple batch
    while (left_idx_ < tuple_batch_left.size()) {
      while (right_idx_ < tuple_batch_right.size()) {
        Value value{plan_->predicate_->EvaluateJoin(&tuple_batch_left[left_idx_], left_schema,
                                                    &tuple_batch_right[right_idx_], right_schema)};

        if (!value.IsNull() && value.GetAs<bool>()) {
          std::vector<Value> values{};
          values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

          for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
            values.push_back(tuple_batch_left[left_idx_].GetValue(&left_schema, i));
          }

          for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
            values.push_back(tuple_batch_right[right_idx_].GetValue(&right_schema, i));
          }

          tuple_batch->push_back(Tuple{values, &GetOutputSchema()});
          rid_batch->push_back(RID{});
          left_matched_[left_idx_] = true;
        }
        right_idx_++;

        if (tuple_batch->size() == batch_size) {
          break;
        }
      }

      // this even if tuple batch right is empty when all the right side tuple is exhausted
      if (right_idx_ == tuple_batch_right.size()) {
        left_idx_++;

        // important as we want to re loop over entire batch of right tuple
        // for each left tuple before moving on using
        // right_executor_->Next
        if (left_idx_ < tuple_batch_left.size()) {
          right_idx_ = 0;
        }
      }

      if (tuple_batch->size() == batch_size) {
        break;
      }
    }
    // end

    // batch_size left tuple evaluating with batch_size right tuple is completed
    if (right_idx_ == tuple_batch_right.size() && left_idx_ == tuple_batch_left.size()) {
      tuple_batch_right.clear();
      rid_batch_right.clear();

      if (right_ele_exist_) {
        right_ele_exist_ = right_executor_->Next(&tuple_batch_right, &rid_batch_right, batch_size);
      }
      right_idx_ = 0;
      left_idx_ = 0;
    }

    // all  tuple on the right table has been consume by the current batch of left tuple
    if (!right_ele_exist_) {
      // handle empty match for left join
      if (plan_->GetJoinType() == JoinType::LEFT) {
        for (size_t i{0}; i < tuple_batch_left.size(); i++) {
          if (!left_matched_[i]) {
            std::vector<Value> values{};
            values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

            for (size_t a = 0; a < left_schema.GetColumnCount(); a++) {
              values.push_back(tuple_batch_left[i].GetValue(&left_schema, a));
            }
            for (size_t a = 0; a < right_schema.GetColumnCount(); a++) {
              values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(a).GetType()));
            }

            if (tuple_batch->size() < batch_size) {
              tuple_batch->push_back(Tuple{values, &GetOutputSchema()});
              rid_batch->push_back(RID{});
            } else {
              left_matched_arr_.push_back(Tuple{values, &GetOutputSchema()});
            }
          }
        }
      }

      // reseting values
      tuple_batch_left.clear();
      rid_batch_left.clear();
      left_matched_.assign(BUSTUB_BATCH_SIZE, false);
      left_idx_ = 0;

      // next batch of tuples from left table
      if (left_ele_exist_) {
        left_ele_exist_ = left_executor_->Next(&tuple_batch_left, &rid_batch_left, batch_size);
      }

      right_executor_->Init();
      right_ele_exist_ = right_executor_->Next(&tuple_batch_right, &rid_batch_right, batch_size);
    }

    if (tuple_batch->size() == batch_size) {
      return true;
    }
  }

  if (tuple_batch->size() < batch_size && plan_->GetJoinType() == JoinType::LEFT) {
    cleanUp_left_matched();
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
