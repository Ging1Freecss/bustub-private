//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table

 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // use the filter expression , for equal sign i.e (where v1=5)
  auto index_plan_maker =
      [this, &optimized_plan](
          const ComparisonExpression *filter_expr) -> std::optional<std::pair<index_oid_t, AbstractExpressionRef>> {
    const auto &seq_scan_plan{dynamic_cast<const SeqScanPlanNode &>(*optimized_plan)};
    const std::vector<AbstractExpressionRef> &common_exp_plan{filter_expr->children_};
    BUSTUB_ASSERT(common_exp_plan.size() == 2, "must have exactly two children for filter predicate to work");

    const auto *column_child_0{dynamic_cast<const ColumnValueExpression *>(common_exp_plan[0].get())};
    const auto *column_child_1{dynamic_cast<const ColumnValueExpression *>(common_exp_plan[1].get())};

    AbstractExpressionRef value_child;
    const ColumnValueExpression *column_child{nullptr};

    if (column_child_0 != nullptr) {
      value_child = common_exp_plan[1];
      column_child = column_child_0;
    } else {
      value_child = common_exp_plan[0];
      column_child = column_child_1;
    }

    if (column_child == nullptr || value_child == nullptr ||
        dynamic_cast<const ConstantValueExpression *>(value_child.get()) == nullptr) {
      return std::nullopt;
    }

    std::shared_ptr<TableInfo> table_info{catalog_.GetTable(seq_scan_plan.table_oid_)};
    const std::vector<std::shared_ptr<IndexInfo>> &index_info{catalog_.GetTableIndexes(table_info->name_)};

    // get index column name . idea is that if column name are equal,
    // then we should index scan on that column only

    const std::string &index_name{table_info->schema_.GetColumn(column_child->GetColIdx()).GetName()};
    for (const auto &ele : index_info) {
      auto &key_schema{ele->key_schema_};

      if (key_schema.GetColumnCount() == 1 && key_schema.GetColumn(0).GetName() == index_name) {
        return std::pair{ele->index_oid_, value_child};
      }
    }

    return std::nullopt;
  };

  // this parse expression for where v1 = 1 or v2 = 3 or v4 = 9 .... type of expression only
  auto ParseOrExpression = [&](auto &self, const AbstractExpressionRef &expr)
      -> std::optional<std::pair<index_oid_t, std::vector<AbstractExpressionRef>>> {
    const auto *comp_expr{dynamic_cast<const ComparisonExpression *>(expr.get())};
    const auto *logic_expr{dynamic_cast<const LogicExpression *>(expr.get())};

    if (comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
      std::optional<std::pair<index_oid_t, AbstractExpressionRef>> index_plan{index_plan_maker(comp_expr)};

      if (index_plan.has_value()) {
        return std::optional{std::pair{index_plan->first, std::vector{index_plan->second}}};
      }
      return std::nullopt;
    } else if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::Or) {
      std::optional<std::pair<index_oid_t, std::vector<AbstractExpressionRef>>> left_child =
          self(self, logic_expr->GetChildAt(0));

      std::optional<std::pair<index_oid_t, std::vector<AbstractExpressionRef>>> right_child =
          self(self, logic_expr->GetChildAt(1));

      if (left_child.has_value() && right_child.has_value() && left_child->first == right_child->first) {
        left_child->second.reserve(left_child->second.size() + right_child->second.size());
        left_child->second.insert(left_child->second.end(), right_child->second.begin(), right_child->second.end());

        return left_child;
      }

      return std::nullopt;
    }

    return std::nullopt;
  };

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan{dynamic_cast<const SeqScanPlanNode &>(*optimized_plan)};

    if (seq_scan_plan.filter_predicate_ != nullptr) {
      auto result{ParseOrExpression(ParseOrExpression, seq_scan_plan.filter_predicate_)};

      if (result.has_value()) {
        return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                   result->first,  // index_oid
                                                   seq_scan_plan.filter_predicate_, result->second);
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
