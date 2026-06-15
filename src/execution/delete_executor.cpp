//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <memory>
#include <optional>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  is_finished = false;

  table_oid_t table_oid_{plan_->GetTableOid()};
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid_);
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows deleted from the table
 * @param[out] rid_batch The next tuple RID batch produced by the delete (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (is_finished) return false;

  Transaction *txn{exec_ctx_->GetTransaction()};
  TransactionManager *txn_mgr{exec_ctx_->GetTransactionManager()};

  int32_t delete_count{0};
  std::vector<bustub::Tuple> tuple_batch_child;
  std::vector<bustub::RID> rid_batch_child;

  std::string table_name{table_info_->name_};
  // const std::vector<std::shared_ptr<IndexInfo>>
  // &index_info_arr{exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};
  TableHeap *table_heap{table_info_.get()->table_.get()};
  Schema *schema{&table_info_->schema_};

  while (child_executor_->Next(&tuple_batch_child, &rid_batch_child, batch_size)) {
    for (size_t i = 0; i < tuple_batch_child.size(); i++) {
      RID tuple_rid{rid_batch_child[i]};
      // tuple data and meta
      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, tuple_rid);

      if (IsWriteWriteConflict(meta, txn)) {
        txn->SetTainted();
        throw ExecutionException("ww conflict");
      }

      std::optional<UndoLink> undo_link_new{undo_link.value_or(UndoLink{INVALID_TXN_ID, 0})};

      if (meta.ts_ == txn->GetTransactionTempTs()) {
        // self own , undo link is gaurantee to exist
        if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
          UndoLog undo_log{txn_mgr->GetUndoLog(undo_link.value())};
          UndoLog undo_log_new{GenerateUpdatedUndoLog(schema, &tuple, nullptr, undo_log)};
          txn->ModifyUndoLog(undo_link->prev_log_idx_, undo_log_new);
        }
      } else {
        // First touch — undo_link may be nullopt
        UndoLink prev = undo_link.has_value() ? undo_link.value() : UndoLink{INVALID_TXN_ID, 0};
        UndoLog undo_log_new = GenerateNewUndoLog(schema, &tuple, nullptr, meta.ts_, prev);
        undo_link_new = txn->AppendUndoLog(undo_log_new);
      }

      bool isDeleted = UpdateTupleAndUndoLink(
          txn_mgr, tuple_rid, undo_link_new, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), true}, tuple,
          [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
            /*
              First-touch case (AppendUndoLog): The lambda CAN throw here (race condition). But the appended undo log is
              harmless — UpdateUndoLink never ran, so version_info_ never points to it. It's orphaned. Transaction gets
              tainted → aborted → GC cleans it up.

              Self-owned case (ModifyUndoLog): The lambda cannot throw here. Since meta.ts_ == our temp ts, we own the
              tuple. No other transaction can modify it between our ModifyUndoLog and the lambda. The re-check will
              always pass. So the modified undo log is always correct.
            */
            if (IsWriteWriteConflict(meta, txn)) {
              txn->SetTainted();
              throw ExecutionException("ww conflict");
            }

            return true;
          });

      if (isDeleted) {
        txn->AppendWriteSet(table_info_->oid_, tuple_rid);

        /*
          Unlike primary keys, secondary index entries do not anchor version chains. Leaving them around would bloat the
          index with useless pointers to deleted tuples and slow down secondary index scans. Therefore, they are cleaned
          up immediately when a deletion occurs.
        */
        const std::vector<std::shared_ptr<IndexInfo>> &index_info_arr{
            exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};
        for (size_t j = 0; j < index_info_arr.size(); j++) {
          if (!index_info_arr[j]->is_primary_key_) {  // imp
            Tuple key{tuple.KeyFromTuple(table_info_->schema_, *index_info_arr[j]->index_->GetKeySchema(),
                                         index_info_arr[j]->index_->GetKeyAttrs())};
            index_info_arr[j]->index_->DeleteEntry(key, tuple_rid, exec_ctx_->GetTransaction());
          }
        }
        delete_count++;
      }
    }
    tuple_batch_child.clear();
    rid_batch_child.clear();
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, delete_count);
  tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});

  is_finished = true;
  return true;
}

}  // namespace bustub
