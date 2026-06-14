//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/comparison_expression.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  BUSTUB_ASSERT(entry_a.first.size() == entry_b.first.size() && order_bys_.size() == entry_a.first.size(),
                "value array size are not equal");
  const std::vector<bustub::Value> &values_a{entry_a.first};
  const std::vector<bustub::Value> &values_b{entry_b.first};

  for (size_t i{0}; i < order_bys_.size(); i++) {
    OrderByType order_type{std::get<0>(order_bys_[i])};
    OrderByNullType null_type{std::get<1>(order_bys_[i])};

    const Value &val_a{values_a[i]};
    const Value &val_b{values_b[i]};

    bool is_asc{order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT};
    bool null_first{null_type == OrderByNullType::NULLS_FIRST};  // if null first then true else if null last then false

    if (null_type == OrderByNullType::DEFAULT) {
      null_first = is_asc;  // for default same as order by val
    }

    if (val_a.IsNull() && val_b.IsNull()) {
      continue;
    } else if (val_a.IsNull()) {
      return null_first;  //  a is null -> a come first if we return true  -> null_first=true needed
    } else if (val_b.IsNull()) {
      return !null_first;  //  b is null -> b come first if we return false -> null_first = true needed
    }

    if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
      return is_asc;
    } else if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
      return !is_asc;
    }  // equal then go to next loop
  }
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey values;

  for (const OrderBy &ele : order_bys) {
    const AbstractExpressionRef &expr = std::get<2>(ele);
    values.push_back(expr->Evaluate(&tuple, schema));
  }
  return values;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<bustub::Value> values{};

  for (uint32_t i{0}; i < schema->GetColumnCount(); i++) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }

  bool is_deleted{base_meta.is_deleted_};

  for (const auto &log : undo_logs) {
    is_deleted = log.is_deleted_;

    BUSTUB_ASSERT(log.modified_fields_.size() == schema->GetColumnCount(),
                  "log.modified_fields_.size() != schema->GetColumnCount()");

    std::vector<uint32_t> attrs{};
    for (size_t i{0}; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        attrs.push_back(i);
      }
    }

    Schema partial_schema{Schema::CopySchema(schema, attrs)};
    size_t partial_index{0};

    for (size_t i{0}; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        values[i] = log.tuple_.GetValue(&partial_schema, partial_index);
        partial_index++;
      }
    }
  }

  if (is_deleted) return std::nullopt;

  return Tuple{values, schema};
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  // Base tuple is visible (no undo needed)
  if (base_meta.ts_ <= txn->GetReadTs() && base_meta.ts_ < TXN_START_ID) {
    return std::vector<UndoLog>{};
  }

  // Base tuple was modified by us (the current transaction)
  if (base_meta.ts_ == txn->GetTransactionTempTs()) {
    return std::vector<UndoLog>{};
  }

  if (!undo_link.has_value() || !undo_link->IsValid()) {
    return std::nullopt;
  }

  // Base tuple is newer than us or owned by another uncommitted transaction

  std::vector<UndoLog> undo_logs{};

  std::optional<UndoLink> current_link = undo_link;

  while (current_link.has_value() && current_link->IsValid()) {
    std::optional<UndoLog> current_undo_log{txn_mgr->GetUndoLogOptional(current_link.value())};

    // means we reach end of loop and could not find ts <= read_ts -> there is no tuple this txn can read
    if (!current_undo_log.has_value()) {
      break;
    }

    undo_logs.push_back(current_undo_log.value());

    if (current_undo_log->ts_ <= txn->GetReadTs()) {
      return undo_logs;
    }

    current_link = current_undo_log->prev_version_;
  }

  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  if (base_tuple == nullptr) {
    // mean it is deleted
    return UndoLog{true, std::vector<bool>(schema->GetColumnCount(), false), Tuple{}, ts, prev_version};
  } else if (target_tuple == nullptr) {
    // target tuple is deleted
    return UndoLog{false, std::vector<bool>(schema->GetColumnCount(), true), *base_tuple, ts, prev_version};
  } else if (base_tuple != nullptr && target_tuple != nullptr) {
    std::vector<bool> modified_fields(schema->GetColumnCount(), false);
    std::vector<Value> values;
    std::vector<uint32_t> attrs;

    for (size_t i{0}; i < schema->GetColumnCount(); i++) {
      if (base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i)) == false) {
        modified_fields[i] = true;
        values.push_back(base_tuple->GetValue(schema, i));
        attrs.push_back(i);
      }
    }

    Schema partial_schema{Schema::CopySchema(schema, attrs)};
    Tuple partial_tuple{values, &partial_schema};
    return UndoLog{false, modified_fields, partial_tuple, ts, prev_version};
  }

  throw bustub::Exception("this should never happen base tuple and target tuple both null ptr");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // means original was deleted , so new undo log is just deleted
  if (log.is_deleted_) {
    return UndoLog{true, std::vector<bool>(schema->GetColumnCount(), false), Tuple{}, log.ts_, log.prev_version_};
  }

  /*
    If the re-inserted value happens to match the original
    for some column, it would drop that column from the undo log.

    this is important :
      if (target_val.CompareExactlyEquals(log_val) == false || log.modified_fields_[i])

    NOT :
      if (target_val.CompareExactlyEquals(log_val) == false)
  */
  if (target_tuple == nullptr && base_tuple != nullptr) {
    // tuple is deleted so restore to original tuple as every value of tuple is change now

    std::vector<uint32_t> partial_attrs{};

    for (size_t i{0}; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        partial_attrs.push_back(i);
      }
    }

    Schema partial_schema{Schema::CopySchema(schema, partial_attrs)};

    std::vector<Value> values{};

    size_t j{0};
    for (size_t i{0}; i < log.modified_fields_.size(); i++) {
      if (log.modified_fields_[i]) {
        values.push_back(log.tuple_.GetValue(&partial_schema, j));
        j++;
      } else {
        values.push_back(base_tuple->GetValue(schema, i));
      }
    }

    Tuple new_log_tuple{values, schema};
    return UndoLog{log.is_deleted_, std::vector<bool>(schema->GetColumnCount(), true), new_log_tuple, log.ts_,
                   log.prev_version_};
  }

  // target tuple is not null so only thing that matter is difference between target tuple and log.tuple
  if (target_tuple != nullptr && base_tuple == nullptr) {
    // since base tuple is deleted this mean log.tuple_ is a full tuple, not just difference
    // so we need to get the difference between log.tuple and target tuple

    std::vector<bool> modified_fields(schema->GetColumnCount(), false);
    std::vector<uint32_t> attrs{};
    std::vector<Value> values{};

    for (size_t i{0}; i < schema->GetColumnCount(); i++) {
      Value target_val{target_tuple->GetValue(schema, i)};
      Value log_val{log.tuple_.GetValue(schema, i)};

      if (target_val.CompareExactlyEquals(log_val) == false || log.modified_fields_[i]) {
        modified_fields[i] = true;
        attrs.push_back(i);
        values.push_back(log_val);
      }
    }

    Schema partial_schema{Schema::CopySchema(schema, attrs)};
    Tuple new_log_tuple{values, &partial_schema};

    return UndoLog{log.is_deleted_, modified_fields, new_log_tuple, log.ts_, log.prev_version_};
  }

  if (target_tuple != nullptr && base_tuple != nullptr) {
    std::vector<uint32_t> partial_attrs{};

    for (size_t i{0}; i < schema->GetColumnCount(); i++) {
      if (log.modified_fields_[i]) {
        partial_attrs.push_back(i);
      }
    }

    Schema partial_schema{Schema::CopySchema(schema, partial_attrs)};

    std::vector<uint32_t> attrs{};
    std::vector<Value> values{};
    std::vector<bool> modified_fields(schema->GetColumnCount(), false);
    size_t j{0};

    for (size_t i{0}; i < schema->GetColumnCount(); i++) {
      Value org_val{};
      Value target_val{target_tuple->GetValue(schema, i)};

      if (log.modified_fields_[i]) {
        org_val = log.tuple_.GetValue(&partial_schema, j);
        j++;
      } else {
        org_val = base_tuple->GetValue(schema, i);
      }

      if (target_val.CompareExactlyEquals(org_val) == false || log.modified_fields_[i]) {
        attrs.push_back(i);
        values.push_back(org_val);
        modified_fields[i] = true;
      }
    }

    Schema new_log_schema{Schema::CopySchema(schema, attrs)};
    Tuple new_log_tuple{values, &new_log_schema};

    return UndoLog{log.is_deleted_, modified_fields, new_log_tuple, log.ts_, log.prev_version_};
  }

  return log;
}

auto IsWriteWriteConflict(const TupleMeta &meta, Transaction *txn) -> bool {
  if (meta.ts_ >= TXN_START_ID) {
    if (meta.ts_ == txn->GetTransactionTempTs())
      // Self-modification
      return false;
    else
      return true;
  }

  if (meta.ts_ <= txn->GetReadTs())
    // visible committed version
    return false;
  else
    // Committed version is newer than our read_ts
    return true;
}

auto GenerateEmptyTuple(const Schema *schema) -> Tuple {
  std::vector<Value> values{};
  for (const Column &col : schema->GetColumns()) {
    values.emplace_back(ValueFactory::GetZeroValueByType(col.GetType()));
  }

  return Tuple{values, schema};
}

auto version_chain_has_conflict(const RID &rid, const std::vector<std::shared_ptr<bustub::AbstractExpression>> &preds,
                                const Schema &schema, TableHeap *table_heap, TransactionManager *txn_mgr,
                                timestamp_t read_ts) -> bool {
  auto [meta, tuple, undo_link_opt] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);

  Tuple after_tuple = tuple;
  bool after_deleted = meta.is_deleted_;

  std::optional<UndoLink> link = undo_link_opt;

  while (link.has_value() && link->IsValid()) {
    auto undo_log_opt{txn_mgr->GetUndoLogOptional(link.value())};

    if (!undo_log_opt.has_value()) {
      break;
    }

    UndoLog undo_log = std::move(undo_log_opt.value());

    if (undo_log.ts_ <= read_ts) {
      break;
    }

    bool before_deleted = undo_log.is_deleted_;
    Tuple before_tuple{GenerateEmptyTuple(&schema)};

    if (!before_deleted) {
      // this fn return nullopt if tuple is deleted, so if before_deleted = true -> nullopt ,
      auto before_tuple_opt = ReconstructTuple(&schema, after_tuple, meta, std::vector<UndoLog>{undo_log});

      if (before_tuple_opt.has_value()) {
        before_tuple = before_tuple_opt.value();
      }
    }

    if (after_deleted && before_deleted) {
      continue;
    } else if (after_deleted && (!before_deleted)) {
      if (match_any_pred(before_tuple, preds, schema)) {
        return true;
      }
    } else if (!after_deleted && before_deleted) {
      if (match_any_pred(after_tuple, preds, schema)) {
        return true;
      }
    } else {
      if (match_any_pred(before_tuple, preds, schema) || match_any_pred(after_tuple, preds, schema)) {
        return true;
      }
    }

    after_tuple = std::move(before_tuple);
    after_deleted = before_deleted;
    link = undo_log.prev_version_;
    meta = TupleMeta{undo_log.ts_, before_deleted};
  }
  return false;
}

auto match_any_pred(const Tuple &tuple, const std::vector<std::shared_ptr<bustub::AbstractExpression>> &preds,
                    const Schema &schema) -> bool {
  for (const auto &pred : preds) {
    //  query do a complete scan -> this tuple is always included
    if (pred.get() == nullptr) {
      return true;
    }

    Value val = pred->Evaluate(&tuple, schema);

    if (!val.IsNull() && val.GetAs<bool>()) {
      return true;
    }
  }

  return false;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
