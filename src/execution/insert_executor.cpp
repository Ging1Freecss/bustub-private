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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/rid.h"
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
  std::string table_name{table_info_ptr->name_};

  const std::vector<std::shared_ptr<IndexInfo>> &index_info_arr{exec_ctx_->GetCatalog()->GetTableIndexes(table_name)};

  std::vector<bustub::Tuple> tuple_batch_child;
  std::vector<bustub::RID> rid_batch_child;

  int32_t total_size{0};

  while (child_executor_->Next(&tuple_batch_child, &rid_batch_child, batch_size)) {
    for (int i = 0; i < (int)tuple_batch_child.size(); i++) {
      std::optional<RID> rid{table_info_ptr->table_->InsertTuple(TupleMeta{0, false}, tuple_batch_child[i])};

      if (rid.has_value()) {
        for (int j = 0; j < int(index_info_arr.size()); j++) {
          Tuple tuple_extract{tuple_batch_child[i].KeyFromTuple(child_executor_->GetOutputSchema(),
                                                                *index_info_arr[j]->index_->GetKeySchema(),
                                                                index_info_arr[j]->index_->GetKeyAttrs())};

          index_info_arr[j]->index_->InsertEntry(tuple_extract, rid.value(), exec_ctx_->GetTransaction());
        }

        total_size += 1;
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
