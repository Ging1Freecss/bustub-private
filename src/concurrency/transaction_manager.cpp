//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <atomic>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(P4): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_.store(last_commit_ts_);
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this function and you can change / remove it as
 * you want. */

/*
  1.find the conflicting txn , which have commit ts > ts.read_ts_ , means current txn has not seen those commits
  2.from the write set of conflicting txn, collect modified rids
  3.check rid of each tuple, if it is commited after read ts, if it is then abort
*/
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->scan_predicates_.empty()) {
    return true;
  }

  std::vector<std::shared_ptr<Transaction>> conflict_txn{};
  std::unordered_map<table_oid_t, std::unordered_set<RID>> modified_rids;

  for (const auto &[txn_id, other_txn] : txn_map_) {
    if (other_txn->state_ == TransactionState::COMMITTED && other_txn->commit_ts_ > txn->read_ts_) {
      conflict_txn.push_back(other_txn);
    }
  }

  for (const auto &other_txn : conflict_txn) {
    for (const auto &[table_oid, rid_sets] : other_txn->GetWriteSets()) {
      modified_rids[table_oid].insert(rid_sets.begin(), rid_sets.end());
    }
  }

  for (auto &[table_oid, predicate] : txn->scan_predicates_) {
    if (!modified_rids.count(table_oid)) {
      continue;
    }

    const Schema &schema{catalog_->GetTable(table_oid)->schema_};
    TableHeap *table_heap{catalog_->GetTable(table_oid)->table_.get()};

    for (const auto &rid : modified_rids[table_oid]) {
      if (version_chain_has_conflict(rid, predicate, schema, table_heap, this, txn->read_ts_)) {
        return false;
      }
    }
  }
  return true;
}

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn manager so no need to delete it by
 * yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(P4): acquire commit ts!
  timestamp_t last_commit_ts{last_commit_ts_.load() + 1};

  if (txn->state_ != TransactionState::RUNNING) {
    commit_lck.unlock();
    Abort(txn);
    return false;
    // throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(P4): Implement the commit logic!

  for (const auto &[t_oid, rid_set] : txn->write_set_) {
    TableHeap *table_heap{catalog_->GetTable(t_oid)->table_.get()};

    for (const auto &rid : rid_set) {
      TupleMeta meta{table_heap->GetTupleMeta(rid)};
      meta.ts_ = last_commit_ts;
      table_heap->UpdateTupleMeta(meta, rid);
    }
  }

  txn->commit_ts_.store(last_commit_ts);
  last_commit_ts_.store(last_commit_ts);

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(P4): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(last_commit_ts);
  running_txns_.RemoveTxn(txn->read_ts_);

  lck.unlock();  // important as we will use GetUndoLogOptional below which uses the same lock

  // garbage collection for write set txn
  // timestamp_t w{GetWatermark()};

  // for (const auto &[_, rid_set] : txn->write_set_) {
  //   for (const auto &rid : rid_set) {
  //     std::optional<UndoLink> undo_link{GetUndoLink(rid)};
  //     bool found_last_undo_log{false};

  //     while (undo_link.has_value() && undo_link->IsValid()) {
  //       std::optional<UndoLog> undo_log{GetUndoLogOptional(undo_link.value())};

  //       if (!undo_log.has_value()) {
  //         break;
  //       }

  //       if (undo_log->ts_ <= w) {
  //         if (!found_last_undo_log) {
  //           found_last_undo_log = true;
  //         } else {
  //           std::shared_lock<std::shared_mutex> gc_lck(txn_map_mutex_);
  //           if (auto it = txn_map_.find(undo_link->prev_txn_); it != txn_map_.end()) {
  //             it->second->ClearUndoLog();
  //           }
  //           gc_lck.unlock();

  //           break;
  //         }
  //       }

  //       undo_link = undo_log->prev_version_;
  //     }
  //   }
  // }
  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(P4): Implement the abort logic!
  {
    for (const auto &[table_oid, rid_sets] : txn->write_set_) {
      std::shared_ptr<TableInfo> table_info{catalog_->GetTable(table_oid)};
      TableHeap *table_heap{table_info->table_.get()};
      Schema *schema{&table_info->schema_};

      for (const RID &rid : rid_sets) {
        auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table_heap, rid);

        // not txn tuple
        if (meta.ts_ != txn->GetTransactionTempTs()) {
          continue;
        }

        if (undo_link.has_value() && undo_link->prev_txn_ == txn->txn_id_) {
          // txn has updated the tuple
          std::optional<UndoLog> undo_log{GetUndoLogOptional(undo_link.value())};

          if (!undo_log.has_value()) {
            continue;
          }

          if (undo_log->is_deleted_) {
            TupleMeta new_meta{undo_log->ts_, true};
            std::optional<UndoLink> new_undo_link =
                undo_log->prev_version_.IsValid() ? std::optional<UndoLink>{undo_log->prev_version_} : std::nullopt;

            UpdateTupleAndUndoLink(this, rid, new_undo_link, table_heap, txn, new_meta, GenerateEmptyTuple(schema));
          } else {
            std::optional<Tuple> opt_new_tuple{
                ReconstructTuple(schema, tuple, meta, std::vector<UndoLog>{undo_log.value()})};

            std::optional<UndoLink> new_undo_link =
                undo_log->prev_version_.IsValid() ? std::optional<UndoLink>{undo_log->prev_version_} : std::nullopt;

            UpdateTupleAndUndoLink(this, rid, new_undo_link, table_heap, txn, TupleMeta{undo_log->ts_, false},
                                   opt_new_tuple.value());
          }
        } else {
          // txn has inserted the tuple, no undo log
          TupleMeta new_meta{0, true};
          UpdateTupleAndUndoLink(this, rid, std::nullopt, table_heap, txn, new_meta, GenerateEmptyTuple(schema));
        }
      }
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all transactions are not accessing the table
 * heap. */
void TransactionManager::GarbageCollection() {
  timestamp_t water_mark{GetWatermark()};
  std::unordered_set<txn_id_t> txn_id_st;

  std::vector<std::string> table_names{catalog_->GetTableNames()};

  for (const std::string &table_name : table_names) {
    std::shared_ptr<TableInfo> table_info{catalog_->GetTable(table_name)};

    TableIterator table_itr = table_info->table_->MakeIterator();

    while (!table_itr.IsEnd()) {
      auto [meta, tuple] = table_itr.GetTuple();
      RID rid{table_itr.GetRID()};

      // if heap
      //   version is already visible at watermark, all the undo log txn need to be remove
      if (meta.ts_ <= water_mark) {
        ++table_itr;
        continue;
      }
      std::optional<UndoLink> undo_link{GetUndoLink(rid)};

      while (undo_link.has_value() && undo_link->IsValid()) {
        std::optional<UndoLog> undo_log{GetUndoLogOptional(undo_link.value())};

        if (!undo_log.has_value()) {
          break;
        }

        txn_id_st.insert(undo_link->prev_txn_);

        // we insert the first txn id that satisfy ts_ <= water_mark , ignore rest and we dont need them
        if (undo_log->ts_ <= water_mark) {
          break;
        }

        undo_link = undo_log->prev_version_;
      }
      ++table_itr;
    }
  }

  std::vector<txn_id_t> to_remove;
  for (const auto &[txn_id, txn_ptr] : txn_map_) {
    if ((txn_ptr->state_ == TransactionState::COMMITTED || txn_ptr->state_ == TransactionState::ABORTED) &&
        txn_id_st.count(txn_id) == 0) {
      to_remove.push_back(txn_id);
    }
  }
  for (auto id : to_remove) {
    txn_map_.erase(id);
  }
}

}  // namespace bustub
