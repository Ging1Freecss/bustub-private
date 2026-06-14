//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  plan_ = plan;
}

/** Initialize the update */
void UpdateExecutor::Init() {
  child_executor_->Init();
  table_oid_t table_oid{plan_->GetTableOid()};
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_oid).get();
  is_updated = false;
  tuple_batch_child.clear();
  rid_batch_child.clear();

  std::vector<Tuple> batch_tuples;
  std::vector<RID> batch_rids;
  while (child_executor_->Next(&batch_tuples, &batch_rids, BUSTUB_BATCH_SIZE)) {
    for (size_t i = 0; i < batch_tuples.size(); i++) {
      tuple_batch_child.push_back(batch_tuples[i]);
      rid_batch_child.push_back(batch_rids[i]);
    }
    batch_tuples.clear();
    batch_rids.clear();
  }
}

/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (is_updated) return false;

  int32_t update_count{0};

  table_oid_t table_oid{plan_->GetTableOid()};
  std::shared_ptr<TableInfo> table_info_ptr{exec_ctx_->GetCatalog()->GetTable(table_oid)};
  Transaction *txn{exec_ctx_->GetTransaction()};
  TransactionManager *txn_mgr{exec_ctx_->GetTransactionManager()};

  std::string table_name{table_info_->name_};
  TableHeap *table_heap{table_info_->table_.get()};
  const Schema *schema{&table_info_->schema_};
  const std::vector<std::shared_ptr<IndexInfo>> &index_info_arr{exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};

  // if primary key is updating
  std::vector<Tuple> defer_delete;

  for (size_t i = 0; i < tuple_batch_child.size(); i++) {
    const std::vector<AbstractExpressionRef> &expr{plan_->target_expressions_};

    std::vector<Value> values;
    for (size_t j = 0; j < expr.size(); j++) {
      Value val{expr[j]->Evaluate(&tuple_batch_child[i], child_executor_->GetOutputSchema())};
      values.push_back(val);
    }

    // create the new tuple to update
    Tuple new_tuple{values, &table_info_->schema_};

    const RID rid{rid_batch_child[i]};
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);

    if (IsWriteWriteConflict(meta, txn)) {
      txn->SetTainted();
      throw ExecutionException("ww conflict");
    }

    bool isKeyUpdating{false};

    // check if primary id is changing
    for (size_t j = 0; j < index_info_arr.size(); j++) {
      /*
        dont delete the old key before inserting the new key as prev transaction may need it
      */
      if (!index_info_arr[j]->is_primary_key_) {
        continue;
      }
      Tuple old_key{tuple.KeyFromTuple(*schema, *index_info_arr[j]->index_->GetKeySchema(),
                                       index_info_arr[j]->index_->GetKeyAttrs())};
      Tuple new_key{new_tuple.KeyFromTuple(*schema, *index_info_arr[j]->index_->GetKeySchema(),
                                           index_info_arr[j]->index_->GetKeyAttrs())};

      const Schema *key_schema{index_info_arr[j]->index_->GetKeySchema()};
      const size_t column_count{index_info_arr[j]->index_->GetKeySchema()->GetColumnCount()};

      for (size_t idx{0}; idx < column_count; idx++) {
        if (old_key.GetValue(key_schema, idx).CompareExactlyEquals(new_key.GetValue(key_schema, idx)) == false) {
          isKeyUpdating = true;
          break;
        }
      }

      if (isKeyUpdating) {
        break;
      }
    }

    if (!isKeyUpdating) {
      std::optional<UndoLink> undo_link_new{undo_link.value_or(UndoLink{INVALID_TXN_ID, 0})};
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        // self own
        if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
          UndoLog undo_log{txn_mgr->GetUndoLog(undo_link.value())};
          UndoLog undo_log_new{GenerateUpdatedUndoLog(schema, &tuple, &new_tuple, undo_log)};
          txn->ModifyUndoLog(undo_link->prev_log_idx_, undo_log_new);
        }
      } else {
        // First touch — undo_link may be nullopt
        UndoLink prev = undo_link.has_value() ? undo_link.value() : UndoLink{INVALID_TXN_ID, 0};
        UndoLog undo_log_new = GenerateNewUndoLog(schema, &tuple, &new_tuple, meta.ts_, prev);
        undo_link_new = txn->AppendUndoLog(undo_log_new);
      }

      bool is_success = UpdateTupleAndUndoLink(
          txn_mgr, rid, undo_link_new, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple,
          [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
            if (IsWriteWriteConflict(meta, txn)) {
              txn->SetTainted();
              throw ExecutionException("ww conflict");
            }

            return true;
          });

      /*
          Heap + undo log — unchanged from before. Update in-place at the same RID, create undo log for old values.
          Index — never delete old entries. Only insert a new entry if the primary key value changed.
          Check uniqueness on the new key.
      */
      if (is_success) {
        txn->AppendWriteSet(plan_->GetTableOid(), rid);

        update_count++;
      }
    } else {
      // primary id is changing here
      /* to delete the current tuple before inserting updated tuple*/
      std::optional<UndoLink> undo_link_delete{undo_link.value_or(UndoLink{INVALID_TXN_ID, 0})};

      if (meta.ts_ == txn->GetTransactionTempTs()) {
        // since it is modified by the current txn , insert -> update case
        if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
          UndoLog prev_undo_log{txn_mgr->GetUndoLog(undo_link.value())};
          UndoLog new_undo_log{GenerateUpdatedUndoLog(schema, &tuple, nullptr, prev_undo_log)};
          txn->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
        }
      } else {
        // update only case , need to create undo log
        // First touch — undo_link may be nullopt
        UndoLink prev = undo_link.has_value() ? undo_link.value() : UndoLink{INVALID_TXN_ID, 0};
        UndoLog undo_log_new = GenerateNewUndoLog(schema, &tuple, nullptr, meta.ts_, prev);
        undo_link_delete = txn->AppendUndoLog(undo_log_new);
      }

      // delete the tuple
      bool isDeleted = UpdateTupleAndUndoLink(
          txn_mgr, rid, undo_link_delete, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), true}, tuple,
          [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
            /*
              First-touch case (AppendUndoLog): The lambda CAN throw here (race condition). But the appended undo log
              is harmless — UpdateUndoLink never ran, so version_info_ never points to it. It's orphaned. Transaction
              gets tainted → aborted → GC cleans it up.

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

      if (!isDeleted) {
        txn->SetTainted();
        throw ExecutionException("issue with updating, delete tuple does not work when updating primary id");
      }
      txn->AppendWriteSet(table_oid, rid);

      defer_delete.push_back(new_tuple);
    }
  }

  for (const auto &new_tuple : defer_delete) {
    // insert tuple logic
    bool tuple_already_exist{false};
    RID primary_rid{};

    for (size_t j{0}; j < index_info_arr.size(); j++) {
      if (!index_info_arr[j]->is_primary_key_) {
        continue;
      }
      std::vector<RID> result{};
      Tuple index_key{new_tuple.KeyFromTuple(*schema, *index_info_arr[j]->index_->GetKeySchema(),
                                             index_info_arr[j]->index_->GetKeyAttrs())};
      index_info_arr[j]->index_->ScanKey(index_key, &result, txn);

      if (!result.empty()) {
        tuple_already_exist = true;
        primary_rid = result.front();
      }
    }

    if (tuple_already_exist) {
      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, primary_rid);

      // clear out both non owing and commited part
      if (IsWriteWriteConflict(meta, txn)) {
        txn->SetTainted();
        throw ExecutionException("error when inserting key in index, write write conflict");
      }

      if (meta.is_deleted_ == false) {
        txn->SetTainted();
        throw ExecutionException(
            "tuple is already inserted , can't insert multiple times : unique constraint violation");
      }

      // logically one of the case bound to be execute
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        // as ts = temp ts, mean we have deleted it earlier  now we're re-inserting
        // basically modifying the tuple, since txn deleted the tuple in the same txn

        if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
          UndoLog prev_undo_log{txn_mgr->GetUndoLog(undo_link.value())};
          UndoLog new_undo_log{GenerateUpdatedUndoLog(schema, nullptr, &new_tuple, prev_undo_log)};
          txn->ModifyUndoLog(undo_link.value().prev_log_idx_, new_undo_log);
        }

        UpdateTupleAndUndoLink(
            txn_mgr, primary_rid, undo_link, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple,
            [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
              if (IsWriteWriteConflict(meta, txn)) {
                txn->SetTainted();
                throw ExecutionException("ww conflict");
              }

              return true;
            });

        txn->AppendWriteSet(table_oid, primary_rid);
        update_count++;

      } else if (meta.ts_ <= txn->GetReadTs() && meta.ts_ < TXN_START_ID) {
        // deleted in the prev txn and is visible to current transaction
        UndoLink prev_undo_link{undo_link.has_value() ? undo_link.value() : UndoLink{INVALID_TXN_ID, 0}};
        UndoLog next_undo_log{GenerateNewUndoLog(schema, nullptr, &new_tuple, meta.ts_, prev_undo_link)};

        UndoLink next_undo_link{txn->AppendUndoLog(next_undo_log)};

        UpdateTupleAndUndoLink(
            txn_mgr, primary_rid, next_undo_link, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), false},
            new_tuple, [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
              if (IsWriteWriteConflict(meta, txn)) {
                txn->SetTainted();
                throw ExecutionException("ww conflict");
              }

              return true;
            });

        txn->AppendWriteSet(table_oid, primary_rid);
        update_count++;
      }

    } else {
      std::optional<RID> rid{
          table_info_ptr->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple)};

      if (rid.has_value()) {
        for (int j = 0; j < int(index_info_arr.size()); j++) {
          Tuple tuple_extract{new_tuple.KeyFromTuple(*schema, *index_info_arr[j]->index_->GetKeySchema(),
                                                     index_info_arr[j]->index_->GetKeyAttrs())};

          bool success =
              index_info_arr[j]->index_->InsertEntry(tuple_extract, rid.value(), exec_ctx_->GetTransaction());

          if (!success && index_info_arr[j]->is_primary_key_) {
            txn->SetTainted();
            throw ExecutionException("error when inserting key in index");
          }
        }

        txn->AppendWriteSet(table_oid, rid.value());
        update_count++;
      }
    }
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, update_count);
  tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});

  is_updated = true;
  return true;
}

}  // namespace bustub
