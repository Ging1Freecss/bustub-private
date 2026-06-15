//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  child_tuple_batch_.clear();
  child_tuple_batch_.reserve(BUSTUB_BATCH_SIZE);
  rid_batch_child.clear();
  rid_batch_child.reserve(BUSTUB_BATCH_SIZE);

  match_tuple_.clear();
  match_tuple_idx_ = 0;

  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

  child_idx_ = 0;
  is_finished = false;
  child_exist = child_executor_->Next(&child_tuple_batch_, &rid_batch_child, BUSTUB_BATCH_SIZE);
}

auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (is_finished) return false;

  const Schema &left_schema{child_executor_->GetOutputSchema()};
  const Schema &right_schema{table_info_->schema_};

  auto insert_tuple{[&]() {
    while ((match_tuple_.size() > match_tuple_idx_) && (tuple_batch->size() < batch_size)) {
      std::vector<Value> values{};
      values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

      auto [tuples, right_exist] = match_tuple_[match_tuple_idx_];

      Tuple &left_tuple{tuples.first};
      Tuple &right_tuple{tuples.second};

      for (size_t a = 0; a < left_schema.GetColumnCount(); a++) {
        values.push_back(left_tuple.GetValue(&left_schema, a));
      }

      if (right_exist) {
        for (size_t a = 0; a < right_schema.GetColumnCount(); a++) {
          values.push_back(right_tuple.GetValue(&right_schema, a));
        }
      } else {
        for (size_t a = 0; a < right_schema.GetColumnCount(); a++) {
          values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(a).GetType()));
        }
      }
      tuple_batch->push_back(Tuple{values, &GetOutputSchema()});
      rid_batch->push_back(RID{});

      match_tuple_idx_++;
    }

    if (match_tuple_.size() == match_tuple_idx_) {
      match_tuple_.clear();
      match_tuple_idx_ = 0;
    }
  }};

  while (tuple_batch->size() < batch_size && child_exist) {
    while (child_idx_ < child_tuple_batch_.size() && tuple_batch->size() < batch_size) {
      Tuple &tuple{child_tuple_batch_[child_idx_]};

      // on join clause part
      Value val{plan_->KeyPredicate()->Evaluate(&tuple, child_executor_->GetOutputSchema())};

      if (val.IsNull()) {
        if (plan_->GetJoinType() == JoinType::LEFT) {
          match_tuple_.push_back(std::pair{std::pair{tuple, Tuple{}}, false});
        }
        child_idx_++;
        continue;
      }

      std::vector<Value> values{val};
      Tuple key_tuple{values, index_info_->index_->GetKeySchema()};

      std::vector<bustub::RID> rid_from_right_table;
      index_info_->index_->ScanKey(key_tuple, &rid_from_right_table, exec_ctx_->GetTransaction());

      if (rid_from_right_table.empty() && plan_->GetJoinType() == JoinType::LEFT) {
        match_tuple_.push_back(std::pair{std::pair{tuple, Tuple{}}, false});
      } else if (!rid_from_right_table.empty()) {
        bool is_delete = true;

        std::vector<std::pair<TupleMeta, Tuple>> temp_meta_tuple{};
        for (auto &i : rid_from_right_table) {
          auto [meta, right_tuple] = table_info_->table_->GetTuple(i);
          temp_meta_tuple.push_back({meta, right_tuple});

          if (!meta.is_deleted_) {
            is_delete = false;
          }
        }

        if (is_delete && plan_->GetJoinType() == JoinType::LEFT) {
          match_tuple_.push_back(std::pair{std::pair{tuple, Tuple{}}, false});
        } else if (!is_delete) {
          for (auto &[meta, right_tuple] : temp_meta_tuple) {
            if (!meta.is_deleted_) {
              match_tuple_.push_back(std::pair{std::pair{tuple, right_tuple}, true});
            }
          }
        }
      }

      child_idx_++;

      if (match_tuple_.size() - match_tuple_idx_ >= batch_size - tuple_batch->size()) break;
    }

    insert_tuple();

    if (child_idx_ >= child_tuple_batch_.size()) {
      child_tuple_batch_.clear();
      rid_batch_child.clear();
      child_tuple_batch_.reserve(batch_size);
      rid_batch_child.reserve(batch_size);
      child_idx_ = 0;
      child_exist = child_executor_->Next(&child_tuple_batch_, &rid_batch_child, batch_size);
    }
  }

  if (tuple_batch->size() < batch_size) {
    insert_tuple();
  }

  if (!child_exist && match_tuple_.size() == match_tuple_idx_) {
    is_finished = true;
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
