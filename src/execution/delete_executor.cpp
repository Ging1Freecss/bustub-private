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
#include "catalog/catalog.h"
#include "common/macros.h"
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

  int32_t delete_count{0};
  std::vector<bustub::Tuple> tuple_batch_child;
  std::vector<bustub::RID> rid_batch_child;

  std::string table_name{table_info_->name_};
  const std::vector<std::shared_ptr<IndexInfo>> &index_info_arr{exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};

  while (child_executor_->Next(&tuple_batch_child, &rid_batch_child, batch_size)) {
    for (size_t i = 0; i < tuple_batch_child.size(); i++) {
      RID tuple_rid{tuple_batch_child[i].GetRid()};
      std::pair<TupleMeta, Tuple> tuple_data{table_info_->table_->GetTuple(tuple_rid)};

      // tuple data and meta
      TupleMeta tuple_meta{tuple_data.first};
      Tuple tuple{tuple_data.second};

      table_info_->table_->UpdateTupleMeta(TupleMeta{tuple_meta.ts_, true}, tuple_rid);

      for (size_t j = 0; j < index_info_arr.size(); j++) {
        Tuple key{tuple.KeyFromTuple(table_info_->schema_, *index_info_arr[j]->index_->GetKeySchema(),
                                     index_info_arr[j]->index_->GetKeyAttrs())};

        index_info_arr[j]->index_->DeleteEntry(key, tuple_rid, exec_ctx_->GetTransaction());
      }
      delete_count++;
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
