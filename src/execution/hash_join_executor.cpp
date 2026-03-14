//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstddef>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include "binder/table_ref/bound_join_ref.h"
#include "buffer/arc_replacer.h"
#include "buffer/buffer_pool_manager.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_child_{std::move(left_child)},
      right_child_{std::move(right_child)} {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  // clean up tuple batches
  tuple_batch_left.clear();
  tuple_batch_left.reserve(BUSTUB_BATCH_SIZE);

  tuple_batch_right.clear();
  tuple_batch_right.reserve(BUSTUB_BATCH_SIZE);

  rid_batch_left.clear();
  rid_batch_left.reserve(BUSTUB_BATCH_SIZE);

  rid_batch_right.clear();
  rid_batch_right.reserve(BUSTUB_BATCH_SIZE);

  buffer_.clear();
  buffer_idx_ = 0;

  // grace hash join
  // r:left table
  // s:right table
  r_parts_.assign(B, Partition{});
  s_parts_.assign(B, Partition{});
  left_join_only_.Clear(exec_ctx_->GetBufferPoolManager());
  inner_join_.Clear(exec_ctx_->GetBufferPoolManager());
  left_join_only_idx_ = 0;
  inner_join_idx_ = 0;
  //

  is_finished = false;

  Execute();
}

void HashJoinExecutor::Execute() {
  const Schema &right_schema{right_child_->GetOutputSchema()};

  while (right_child_->Next(&tuple_batch_right, &rid_batch_right, BUSTUB_BATCH_SIZE)) {
    for (Tuple &right_tuple : tuple_batch_right) {
      HashJoinKey key{};
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        key.values.push_back(expr->Evaluate(&right_tuple, right_schema));
      }

      size_t hash_key{key.Hash(Value{TypeId::INTEGER, 0})};
      size_t idx{hash_key % B};
      s_parts_[idx].Insert_Tuple(right_tuple, exec_ctx_->GetBufferPoolManager());
    }
  }
  const Schema &left_schema{left_child_->GetOutputSchema()};

  while (left_child_->Next(&tuple_batch_left, &rid_batch_left, BUSTUB_BATCH_SIZE)) {
    for (Tuple &left_tuple : tuple_batch_left) {
      HashJoinKey key{};
      for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
        key.values.push_back(expr->Evaluate(&left_tuple, left_schema));
      }

      size_t hash_key{key.Hash(Value{TypeId::INTEGER, 0})};
      size_t idx{hash_key % B};
      r_parts_[idx].Insert_Tuple(left_tuple, exec_ctx_->GetBufferPoolManager());
    }
  }

  for (size_t i = 0; i < r_parts_.size(); i++) {
    if (r_parts_[i].partition_pages.empty() && s_parts_[i].partition_pages.empty()) continue;
    GraceHashJoin(r_parts_[i], s_parts_[i]);
  }
}

template <typename T, typename>
void HashJoinExecutor::GraceHashJoin(T &&curr_r_parts, T &&curr_s_parts, Value depth) {
  // for lsp hint
  Partition &r = curr_r_parts;
  Partition &s = curr_s_parts;

  // check in right side of table if it fit entirely in ram
  if (s.FitsInMemory()) {
    BuildAndProbe(std::forward<T>(curr_r_parts), std::forward<T>(curr_s_parts));
    return;
  }

  std::vector<Partition> sub_r_parts(B, Partition{});
  std::vector<Partition> sub_s_parts(B, Partition{});

  BufferPoolManager *bpm_ = exec_ctx_->GetBufferPoolManager();
  const Schema &left_schema{left_child_->GetOutputSchema()};
  const Schema &right_schema{right_child_->GetOutputSchema()};

  for (const page_id_t &page_id : r.partition_pages) {
    std::optional<ReadPageGuard> page_guard{bpm_->CheckedReadPage(page_id, AccessType::Scan)};
    if (!page_guard.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }

    const IntermediateResultPage *page_ptr{page_guard->As<const IntermediateResultPage>()};
    uint32_t end_idx{page_ptr->GetNumTuples()};

    for (uint32_t i{0}; i < end_idx; i++) {
      Tuple left_tuple{page_ptr->GetTuple(i)};
      HashJoinKey key{};
      for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
        key.values.push_back(expr->Evaluate(&left_tuple, left_schema));
      }

      size_t hash_key{key.Hash(depth)};
      size_t idx{hash_key % B};
      sub_r_parts[idx].Insert_Tuple(left_tuple, exec_ctx_->GetBufferPoolManager());
    }
  }

  for (const page_id_t &page_id : s.partition_pages) {
    std::optional<ReadPageGuard> page_guard{bpm_->CheckedReadPage(page_id, AccessType::Scan)};
    if (!page_guard.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }

    const IntermediateResultPage *page_ptr{page_guard->As<const IntermediateResultPage>()};
    uint32_t end_idx{page_ptr->GetNumTuples()};

    for (uint32_t i{0}; i < end_idx; i++) {
      Tuple right_tuple{page_ptr->GetTuple(i)};
      HashJoinKey key{};
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        key.values.push_back(expr->Evaluate(&right_tuple, right_schema));
      }

      size_t hash_key{key.Hash(depth)};
      size_t idx{hash_key % B};
      sub_s_parts[idx].Insert_Tuple(right_tuple, exec_ctx_->GetBufferPoolManager());
    }
  }

  // unpin the pages
  r.Clear(bpm_);
  s.Clear(bpm_);

  for (size_t i{0}; i < sub_r_parts.size(); i++) {
    GraceHashJoin(std::move(sub_r_parts[i]), std::move(sub_s_parts[i]), Value{depth.Add(Value{TypeId::INTEGER, 1})});
  }
}

template <typename T, typename>
void HashJoinExecutor::BuildAndProbe(T &&curr_r_parts, T &&curr_s_parts) {
  // for lsp hint
  Partition &r = curr_r_parts;
  Partition &s = curr_s_parts;

  BufferPoolManager *bpm_{exec_ctx_->GetBufferPoolManager()};
  const Schema &left_schema{left_child_->GetOutputSchema()};
  const Schema &right_schema{right_child_->GetOutputSchema()};

  std::unordered_map<HashJoinKey, std::vector<Tuple>> hash_map;

  for (page_id_t &page_id : s.partition_pages) {
    std::optional<ReadPageGuard> page_guard{bpm_->CheckedReadPage(page_id, AccessType::Scan)};
    if (!page_guard.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }

    const IntermediateResultPage *page_ptr{page_guard->As<const IntermediateResultPage>()};
    uint32_t end_idx{page_ptr->GetNumTuples()};

    for (uint32_t i{0}; i < end_idx; i++) {
      Tuple right_tuple{page_ptr->GetTuple(i)};
      HashJoinKey key{};
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        key.values.push_back(expr->Evaluate(&right_tuple, right_schema));
      }

      hash_map[key].push_back(right_tuple);
    }
  }

  for (page_id_t &page_id : r.partition_pages) {
    std::optional<ReadPageGuard> page_guard{bpm_->CheckedReadPage(page_id, AccessType::Scan)};
    if (!page_guard.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }

    const IntermediateResultPage *page_ptr{page_guard->As<const IntermediateResultPage>()};
    uint32_t end_idx{page_ptr->GetNumTuples()};

    for (uint32_t i{0}; i < end_idx; i++) {
      Tuple left_tuple{page_ptr->GetTuple(i)};
      HashJoinKey key{};
      for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
        key.values.push_back(expr->Evaluate(&left_tuple, left_schema));
      }

      if (auto it = hash_map.find(key); it != hash_map.end()) {
        std::vector<Tuple> &tuple_batch{it->second};

        for (auto &right_tuple : tuple_batch) {
          std::vector<Value> values{};
          values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

          for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
            values.push_back(left_tuple.GetValue(&left_schema, i));
          }

          for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
            values.push_back(right_tuple.GetValue(&right_schema, i));
          }

          Tuple tuple{values, &GetOutputSchema()};
          inner_join_.Insert_Tuple(tuple, bpm_);
        }
      } else if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values{};
        values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

        for (size_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.push_back(left_tuple.GetValue(&left_schema, i));
        }

        for (size_t i = 0; i < right_schema.GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }
        Tuple tuple{values, &GetOutputSchema()};
        left_join_only_.Insert_Tuple(tuple, bpm_);
      }
    }
  }
  r.Clear(bpm_);
  s.Clear(bpm_);
}

/**
 * Yield the next tuple batch from the hash join.
 * @param[out] tuple_batch The next tuple batch produced by the hash join
 * @param[out] rid_batch The next tuple RID batch produced by the hash join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  if (is_finished) return false;
  tuple_batch->clear();
  rid_batch->clear();

  std::vector<page_id_t> &inner_partition_pg = inner_join_.partition_pages;
  std::vector<page_id_t> &left_partition_pg = left_join_only_.partition_pages;

  BufferPoolManager *bpm_{exec_ctx_->GetBufferPoolManager()};

  while (inner_join_idx_ < inner_partition_pg.size() || left_join_only_idx_ < left_partition_pg.size()) {
    while (inner_join_idx_ < inner_partition_pg.size() && buffer_.size() < batch_size) {
      page_id_t page_id{inner_partition_pg[inner_join_idx_]};
      std::optional<ReadPageGuard> page_guard{bpm_->CheckedReadPage(page_id, AccessType::Scan)};
      if (!page_guard.has_value()) {
        throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
      }

      const IntermediateResultPage *page_ptr{page_guard->As<const IntermediateResultPage>()};
      uint32_t end_idx{page_ptr->GetNumTuples()};

      for (uint32_t i{0}; i < end_idx; i++) {
        Tuple tuple{page_ptr->GetTuple(i)};
        buffer_.push_back(tuple);
      }
      inner_join_idx_++;
    }

    if (buffer_.size() >= batch_size) break;

    if (plan_->GetJoinType() == JoinType::LEFT) {
      while (left_join_only_idx_ < left_partition_pg.size() && buffer_.size() < batch_size) {
        page_id_t page_id{left_partition_pg[left_join_only_idx_]};
        std::optional<ReadPageGuard> page_guard{bpm_->CheckedReadPage(page_id, AccessType::Scan)};
        if (!page_guard.has_value()) {
          throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
        }

        const IntermediateResultPage *page_ptr{page_guard->As<const IntermediateResultPage>()};
        uint32_t end_idx{page_ptr->GetNumTuples()};

        for (uint32_t i{0}; i < end_idx; i++) {
          Tuple tuple{page_ptr->GetTuple(i)};
          buffer_.push_back(tuple);
        }
        left_join_only_idx_++;
      }
    }

    if (buffer_.size() >= batch_size) break;
  }

  while (tuple_batch->size() < batch_size && buffer_idx_ < buffer_.size()) {
    auto &tuple = buffer_[buffer_idx_];
    tuple_batch->push_back(tuple);
    rid_batch->push_back(RID{});
    buffer_idx_++;
  }

  if (buffer_.size() == buffer_idx_) {
    buffer_.clear();
    buffer_idx_ = 0;
  }

  is_finished = tuple_batch->empty();
  return !tuple_batch->empty();
}

// first check if partition_pages is empty allocate a page -> insert-> return
// if page exist -> check if can be inserted -> yes insert -> else allocate a new page push_back to partition_pages
// array

void Partition::Insert_Tuple(Tuple &tuple, BufferPoolManager *bpm_) {
  if (partition_pages.empty()) {
    page_id_t page_id{bpm_->NewPage()};
    std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(page_id, AccessType::Lookup)};

    if (!page_gaurd.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }
    partition_pages.push_back(page_id);
    IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
    page_data->Init();
    page_data->InsertTuple(tuple);

    num_tuples++;
    return;
  }

  // for condition when tuple was inserted into the last page of partition
  {
    page_id_t last_page_id{partition_pages.back()};
    std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(last_page_id, AccessType::Lookup)};
    if (!page_gaurd.has_value()) {
      throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
    }
    IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
    bool was_insert = page_data->InsertTuple(tuple);

    if (was_insert) {  // page has space
      num_tuples++;
      return;
    }
  }

  // if not means last page was full
  page_id_t page_id{bpm_->NewPage()};
  std::optional<WritePageGuard> page_gaurd{bpm_->CheckedWritePage(page_id, AccessType::Lookup)};
  if (!page_gaurd.has_value()) {
    throw bustub::ExecutionException(fmt::format("buffer pool manager failed"));
  }
  partition_pages.push_back(page_id);
  IntermediateResultPage *page_data{page_gaurd->AsMut<IntermediateResultPage>()};
  page_data->Init();
  page_data->InsertTuple(tuple);

  num_tuples++;
  return;
}

void Partition::Clear(BufferPoolManager *bpm_) {
  for (page_id_t i : partition_pages) {
    bpm_->DeletePage(i);
  }
  partition_pages.clear();
  num_tuples = 0;
}

auto Partition::FitsInMemory() -> bool {
  // Reserve a few frames for reading probe-side pages
  constexpr int RESERVED_FRAMES = 2;
  return partition_pages.size() <= static_cast<size_t>(B - RESERVED_FRAMES);
}

}  // namespace bustub
