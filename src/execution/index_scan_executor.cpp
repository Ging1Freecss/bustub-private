//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  is_finished = false;
  rids_.clear();

  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());

  if (plan_->pred_keys_.empty()) {
    iter_ = tree_->GetBeginIterator();
  } else {
    // insert rid to later check in Next fn
    for (const AbstractExpressionRef &pred_key : plan_->pred_keys_) {
      Value val{pred_key->Evaluate(nullptr, index_info_->key_schema_)};

      std::vector<Value> value{val};
      Tuple key_tuple{value, &index_info_->key_schema_};
      index_info_->index_->ScanKey(key_tuple, &rids_, exec_ctx_->GetTransaction());
    }
    rid_idx_ = 0;
  }

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    exec_ctx_->GetTransaction()->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (is_finished) return false;

  const Schema &schema{plan_->OutputSchema()};

  TableHeap *table_heap{table_info_->table_.get()};
  Transaction *txn{exec_ctx_->GetTransaction()};
  TransactionManager *txn_mgr{exec_ctx_->GetTransactionManager()};

  if (plan_->pred_keys_.empty()) {
    if (iter_.IsEnd()) return false;

    while (!iter_.IsEnd() && tuple_batch->size() < batch_size) {
      std::pair<const bustub::GenericKey<8>, const bustub::RID> key_rid{*iter_};

      auto [tuple_meta, tuple_data, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, key_rid.second);

      std::optional<std::vector<UndoLog>> undo_logs{
          CollectUndoLogs(key_rid.second, tuple_meta, tuple_data, undo_link, txn, txn_mgr)};

      if (!undo_logs.has_value()) {
        ++iter_;
        continue;
      }

      std::optional<Tuple> tuple_for_curr_txn{ReconstructTuple(&schema, tuple_data, tuple_meta, undo_logs.value())};

      if (!tuple_for_curr_txn.has_value()) {  // if tuple is deleted that case is handle here
        ++iter_;
        continue;
      }

      bool insert_tuple{true};

      if (plan_->filter_predicate_ != nullptr) {
        Value filter_allowed{plan_->filter_predicate_->Evaluate(&tuple_for_curr_txn.value(), table_info_->schema_)};

        if (filter_allowed.IsNull() || !filter_allowed.GetAs<bool>()) {
          insert_tuple = false;
        }
      }

      if (insert_tuple) {
        tuple_batch->push_back(tuple_for_curr_txn.value());
        rid_batch->push_back(key_rid.second);
      }

      ++iter_;
    }

    if (iter_.IsEnd()) {
      is_finished = true;
    }
  } else {
    while (rid_idx_ < rids_.size() && tuple_batch->size() < batch_size) {
      auto [tuple_meta, tuple_data, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rids_[rid_idx_]);

      rid_idx_++;

      std::optional<std::vector<UndoLog>> undo_logs{
          CollectUndoLogs(rids_[rid_idx_ - 1], tuple_meta, tuple_data, undo_link, txn, txn_mgr)};

      if (!undo_logs.has_value()) {
        continue;
      }

      std::optional<Tuple> tuple_for_curr_txn{ReconstructTuple(&schema, tuple_data, tuple_meta, undo_logs.value())};

      if (!tuple_for_curr_txn.has_value()) {  // if tuple is deleted that case is handle here
        continue;
      }

      bool insert_tuple{true};

      if (plan_->filter_predicate_ != nullptr) {
        Value filter_allowed{plan_->filter_predicate_->Evaluate(&tuple_for_curr_txn.value(), table_info_->schema_)};

        if (filter_allowed.IsNull() || !filter_allowed.GetAs<bool>()) {
          insert_tuple = false;
        }
      }

      if (insert_tuple) {
        tuple_batch->push_back(tuple_for_curr_txn.value());
        rid_batch->push_back(rids_[rid_idx_ - 1]);
      }
    }

    if (rid_idx_ >= rids_.size()) {
      is_finished = true;
    }
  }

  return (!tuple_batch->empty());
}

}  // namespace bustub
