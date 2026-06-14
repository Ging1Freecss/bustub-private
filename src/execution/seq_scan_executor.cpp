//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include <memory>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  // get the table iterator
  table_oid_t table_oid{plan_->table_oid_};
  std::shared_ptr<TableInfo> table_info_ptr{exec_ctx_->GetCatalog()->GetTable(table_oid)};
  table_heap_ = table_info_ptr->table_.get();
  table_itr_ = std::make_unique<TableIterator>(table_info_ptr->table_->MakeIterator());

  if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    exec_ctx_->GetTransaction()->AppendScanPredicate(table_oid, plan_->filter_predicate_);
  }
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (table_itr_->IsEnd()) {
    return false;
  }
  const Schema &schema{plan_->OutputSchema()};

  Transaction *txn{exec_ctx_->GetTransaction()};
  TransactionManager *txn_mgr{exec_ctx_->GetTransactionManager()};

  while (!table_itr_->IsEnd() && tuple_batch->size() < batch_size) {
    RID rid{table_itr_->GetRID()};
    auto [tuple_meta, tuple_data, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap_, rid);

    std::optional<std::vector<UndoLog>> undo_logs{
        CollectUndoLogs(table_itr_->GetRID(), tuple_meta, tuple_data, undo_link, txn, txn_mgr)};

    if (!undo_logs.has_value()) {
      ++(*table_itr_);
      continue;
    }

    std::optional<Tuple> tuple_for_curr_txn{ReconstructTuple(&schema, tuple_data, tuple_meta, undo_logs.value())};

    if (!tuple_for_curr_txn.has_value()) {  // if tuple is deleted that case is handle here
      ++(*table_itr_);
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      Value filter_op{plan_->filter_predicate_->Evaluate(&tuple_for_curr_txn.value(), schema)};

      if (filter_op.IsNull() || !filter_op.GetAs<bool>()) {  // check if filter happen
        ++(*table_itr_);
        continue;
      }
    }
    tuple_batch->push_back(tuple_for_curr_txn.value());
    rid_batch->push_back(rid);

    ++(*table_itr_);
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
