//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
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
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  plan_ = plan;
}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  is_finished = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (is_finished) return false;

  table_oid_t table_oid{plan_->GetTableOid()};

  std::shared_ptr<TableInfo> table_info_ptr{exec_ctx_->GetCatalog()->GetTable(table_oid)};
  TableHeap *table_heap{table_info_ptr->table_.get()};
  std::string table_name{table_info_ptr->name_};

  const std::vector<std::shared_ptr<IndexInfo>> &index_info_arr{exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};

  std::vector<bustub::Tuple> tuple_batch_child;
  std::vector<bustub::RID> rid_batch_child;

  int32_t total_size{0};

  Transaction *txn{exec_ctx_->GetTransaction()};
  TransactionManager *txn_mgr{exec_ctx_->GetTransactionManager()};

  while (child_executor_->Next(&tuple_batch_child, &rid_batch_child, batch_size)) {
    for (int i = 0; i < (int)tuple_batch_child.size(); i++) {
      /*
        check if some other transaction has already inserted
        if yes, then set txn to tainted and then throw exception
      */
      bool tuple_already_exist{false};
      RID primary_rid{};
      const Schema schema{child_executor_->GetOutputSchema()};

      for (size_t j{0}; j < index_info_arr.size(); j++) {
        if (!index_info_arr[j]->is_primary_key_) {
          continue;
        }
        std::vector<RID> result{};
        Tuple index_key{tuple_batch_child[i].KeyFromTuple(child_executor_->GetOutputSchema(),
                                                          *index_info_arr[j]->index_->GetKeySchema(),
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
            // Has undo log from prior operation → update it
            UndoLog prev_undo_log{txn_mgr->GetUndoLog(undo_link.value())};
            UndoLog new_undo_log{GenerateUpdatedUndoLog(&schema, nullptr, &tuple_batch_child[i], prev_undo_log)};
            txn->ModifyUndoLog(undo_link.value().prev_log_idx_, new_undo_log);
          }

          UpdateTupleAndUndoLink(
              txn_mgr, primary_rid, undo_link, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), false},
              tuple_batch_child[i],
              [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
                if (IsWriteWriteConflict(meta, txn)) {
                  txn->SetTainted();
                  throw ExecutionException("ww conflict");
                }

                return true;
              });

          txn->AppendWriteSet(table_oid, primary_rid);
          total_size += 1;

        } else if (meta.ts_ <= txn->GetReadTs() && meta.ts_ < TXN_START_ID) {
          // deleted in the prev txn and is visible to current transaction
          UndoLink prev_undo_link{undo_link.has_value() ? undo_link.value() : UndoLink{INVALID_TXN_ID, 0}};
          UndoLog next_undo_log{GenerateNewUndoLog(&schema, nullptr, &tuple_batch_child[i], meta.ts_, prev_undo_link)};

          UndoLink next_undo_link{txn->AppendUndoLog(next_undo_log)};

          UpdateTupleAndUndoLink(
              txn_mgr, primary_rid, next_undo_link, table_heap, txn, TupleMeta{txn->GetTransactionTempTs(), false},
              tuple_batch_child[i],
              [&](const TupleMeta &meta, const Tuple &tuple, RID, std::optional<UndoLink> undo_link) -> bool {
                if (IsWriteWriteConflict(meta, txn)) {
                  txn->SetTainted();
                  throw ExecutionException("ww conflict");
                }

                return true;
              });

          txn->AppendWriteSet(table_oid, primary_rid);
          total_size += 1;
        }

      } else {
        std::optional<RID> rid{
            table_info_ptr->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, tuple_batch_child[i])};

        if (rid.has_value()) {
          txn->AppendWriteSet(table_oid, rid.value());
          for (int j = 0; j < int(index_info_arr.size()); j++) {
            Tuple tuple_extract{tuple_batch_child[i].KeyFromTuple(child_executor_->GetOutputSchema(),
                                                                  *index_info_arr[j]->index_->GetKeySchema(),
                                                                  index_info_arr[j]->index_->GetKeyAttrs())};

            bool success =
                index_info_arr[j]->index_->InsertEntry(tuple_extract, rid.value(), exec_ctx_->GetTransaction());

            if (!success && index_info_arr[j]->is_primary_key_) {
              txn->SetTainted();
              throw ExecutionException("error when inserting key in index");
            }
          }

          total_size += 1;
        }
      }
    }
    tuple_batch_child.clear();
    rid_batch_child.clear();
  }

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, total_size);
  tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});

  is_finished = true;
  return true;
}

}  // namespace bustub
