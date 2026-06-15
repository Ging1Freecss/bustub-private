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

      /*
        n BusTub, the Table Heap always stores the most recently updated version of a tuple. The older versions are
stored as diffs in the Undo Logs.

Let's walk through an example where a tuple's key is updated from Old_Key to New_Key by Transaction A, and your current
transaction is trying to read it.

Scenario: The Table Heap has the New Key
The Update Happens: When Transaction A updates the tuple, it modifies the actual tuple inside the Table Heap so that it
now contains the New_Key. It also creates an Undo Log containing the instructions to revert New_Key back to Old_Key.

Table Heap Tuple: contains New_Key
Undo Log: contains diff to revert to Old_Key
The Index is Updated: The index now has two entries pointing to the same RID:

Old_Key -> RID
New_Key -> RID
Your Transaction Scans the Index: Now, imagine your current transaction started after Transaction A committed. This
means your transaction's read timestamp allows it to see Transaction A's changes.

Scanning the Old_Key Entry:

The scanner looks at Old_Key -> RID.
It goes to the Table Heap using that RID.
It reads the base tuple from the heap, which currently contains the New_Key!
It calls ReconstructTuple. Because your transaction's timestamp is newer than Transaction A's commit timestamp,
ReconstructTuple says, "I don't need to apply any undo logs. The base tuple in the heap is exactly what this transaction
should see." Result: The reconstructed tuple has the New_Key. The Mismatch (Old_Key != New_Key): At this point, the
scanner is holding an index entry for Old_Key, but the reconstructed tuple has New_Key. Because they don't match, the
code sets insert_tuple = false. This correctly ignores the Old_Key index entry because that index entry is now "stale"
from the perspective of your current transaction.

Scanning the New_Key Entry:

Next, the scanner visits New_Key -> RID.
It goes to the heap, reads the base tuple (New_Key), and ReconstructTuple returns the base tuple.
Now, the index entry key (New_Key) matches the reconstructed tuple's key (New_Key).
insert_tuple remains true, and the tuple is successfully yielded!
What if your transaction was older?
If your transaction started before Transaction A updated the tuple, ReconstructTuple would apply the undo log. The
reconstructed tuple would then have the Old_Key. In that case:

When scanning the Old_Key entry, the keys would match, and you would yield the old tuple.
When scanning the New_Key entry, the keys would not match (New_Key != Old_Key), so you would correctly ignore the new
index entry!
      */
      Tuple tuple_key = tuple_for_curr_txn->KeyFromTuple(table_info_->schema_, *index_info_->index_->GetKeySchema(),
                                                         index_info_->index_->GetKeyAttrs());
      bustub::GenericKey<8> tuple_generic_key;
      tuple_generic_key.SetFromKey(tuple_key);
      bustub::GenericComparator<8> comparator(index_info_->index_->GetKeySchema());

      if (comparator(tuple_generic_key, key_rid.first) != 0) {
        insert_tuple = false;
      }

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
