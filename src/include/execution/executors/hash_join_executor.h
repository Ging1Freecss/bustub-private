//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "binder/table_ref/bound_join_ref.h"
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {
// 1. Define HashJoinKey FIRST (inside bustub namespace)

template <typename>
struct is_vector : std::false_type {};

template <typename T, typename A>
struct is_vector<std::vector<T, A>> : std::true_type {};

struct HashJoinKey {
 public:
  std::vector<Value> values;
  HashJoinKey() = default;
  HashJoinKey(std::vector<Value> values_) : values{std::move(values_)} {}

  auto Hash(Value depth = Value{TypeId::INTEGER, 0}) -> size_t {
    size_t curr_hash = 0;
    for (const auto &key : values) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }

    if (depth.CompareGreaterThan(Value{TypeId::INTEGER, 0}) == CmpBool::CmpTrue) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&depth));
    }
    return curr_hash;
  }

  auto operator==(const HashJoinKey &other) const -> bool {
    if (other.values.size() != values.size()) {
      return false;
    }
    for (uint32_t i = 0; i < other.values.size(); i++) {
      if (values[i].CompareEquals(other.values[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.values) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

// constexpr int B = 4; test even for this mean recursive partioning is working
constexpr int B = 128;
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */

class Partition {
 public:
  std::vector<page_id_t> partition_pages{};
  size_t num_tuples{0};

  Partition() = default;
  ~Partition() = default;

  Partition(const Partition &other) : num_tuples(other.num_tuples) {
    partition_pages.assign(other.partition_pages.begin(), other.partition_pages.end());
  }

  Partition(Partition &&other) : partition_pages(std::move(other.partition_pages)), num_tuples(other.num_tuples) {}

  Partition &operator=(const Partition &other) {
    if (this != &other) {
      partition_pages.clear();

      partition_pages.assign(other.partition_pages.begin(), other.partition_pages.end());
      num_tuples = other.num_tuples;
    }
    return *this;
  }

  Partition &operator=(Partition &&other) {
    if (this != &other) {
      partition_pages.clear();
      partition_pages = std::move(other.partition_pages);
      num_tuples = other.num_tuples;
    }
    return *this;
  }

  void Insert_Tuple(Tuple &tuple, BufferPoolManager *bpm_);
  void Clear(BufferPoolManager *bpm_);
  auto FitsInMemory() -> bool;
};

class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };
  void Execute();

  template <typename T, typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, Partition>>>
  void GraceHashJoin(T &&curr_r_parts, T &&curr_s_parts, Value depth = Value{TypeId::INTEGER, 0});

  template <typename T, typename = std::enable_if_t<std::is_same_v<std::decay_t<T>, Partition>>>
  void BuildAndProbe(T &&curr_r_parts, T &&curr_s_parts);

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_{nullptr};
  std::unique_ptr<AbstractExecutor> right_child_{nullptr};

  // current batch of values
  std::vector<bustub::Tuple> tuple_batch_left{};
  std::vector<bustub::Tuple> tuple_batch_right{};
  std::vector<bustub::RID> rid_batch_left{};
  std::vector<bustub::RID> rid_batch_right{};

  bool is_finished{false};

  // buffer to store left and right table tuple with type of join

  std::vector<Tuple> buffer_{};
  size_t buffer_idx_{0};

  /** @brief Grace Hash Join */
  std::vector<Partition> r_parts_;
  std::vector<Partition> s_parts_;

  Partition left_join_only_;
  size_t left_join_only_idx_{0};

  Partition inner_join_;
  // current idx in inner_join.partition_page vector
  size_t inner_join_idx_{0};
};

}  // namespace bustub
