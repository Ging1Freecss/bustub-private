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
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
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
  std::vector<bustub::Tuple> tuple_batch_child;
  std::vector<bustub::RID> rid_batch_child;

  std::string table_name{table_info_->name_};
  const std::vector<std::shared_ptr<IndexInfo>> &index_info_arr{exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};

  while (child_executor_->Next(&tuple_batch_child, &rid_batch_child, batch_size)) {
    for (size_t i = 0; i < tuple_batch_child.size(); i++) {
      const std::vector<AbstractExpressionRef> &expr{plan_->target_expressions_};

      std::vector<Value> values;
      for (size_t j = 0; j < expr.size(); j++) {
        Value val{expr[j]->Evaluate(&tuple_batch_child[i], child_executor_->GetOutputSchema())};
        values.push_back(val);
      }

      const RID rid{rid_batch_child[i]};
      const TupleMeta tuple_meta{table_info_->table_->GetTupleMeta(rid)};
      // create the new tuple to update
      Tuple new_tuple{values, &table_info_->schema_};

      // mark old tuple data delete
      TupleMeta tuple_meta_delete{tuple_meta.ts_, true};
      table_info_->table_->UpdateTupleMeta(tuple_meta_delete, rid);

      // insert the new tuple
      std::optional<RID> new_rid{table_info_->table_->InsertTuple(tuple_meta, new_tuple)};

      // update the indexes
      if (new_rid.has_value()) {
        for (size_t j = 0; j < index_info_arr.size(); j++) {
          Tuple old_tuple{tuple_batch_child[i].KeyFromTuple(child_executor_->GetOutputSchema(),
                                                            *index_info_arr[j]->index_->GetKeySchema(),
                                                            index_info_arr[j]->index_->GetKeyAttrs())};
          index_info_arr[j]->index_->DeleteEntry(old_tuple, rid, exec_ctx_->GetTransaction());

          Tuple new_key{new_tuple.KeyFromTuple(table_info_->schema_, *index_info_arr[j]->index_->GetKeySchema(),
                                               index_info_arr[j]->index_->GetKeyAttrs())};

          index_info_arr[j]->index_->InsertEntry(new_key, new_rid.value(), exec_ctx_->GetTransaction());
        }
        update_count++;
      }
    }  // for loop end
    tuple_batch_child.clear();
    rid_batch_child.clear();
  }
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, update_count);
  tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});

  is_updated = true;
  return true;
}

}  // namespace bustub
