//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstddef>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/*
  At its core, tuple_idx = 0 is an index that tells an expression evaluator to get data from the first child (or the
only child) of the current plan node.

1. In Single-Child Nodes (The Common Case)
You see tuple_idx = 0 everywhere because most query plan operators—like Filter, Projection, and Limit—have only one
input child. Since there's only one source of tuples for them to process, it is always referenced by index 0. There is
no tuple_idx = 1 because there is no second child. code Code Filter (Predicate using tuple_idx=0)

Child Node (The source of data for tuple_idx=0)

2. In Multi-Child Nodes (The Join Case)
A Join node is the classic example of an operator with two children. It must distinguish between them:
tuple_idx = 0 refers to the left child.
tuple_idx = 1 refers to the right child.

3. The Critical "Filter on a Join" Scenario
The confusion arises when these two cases are combined. When a Filter node sits on top of a Join node:
From the Filter's perspective: It has only one child (the join itself). It receives a single stream of wide tuples from
the join. Therefore, any column it accesses in its predicate, regardless of its original table, will use tuple_idx = 0.
From the Join's perspective: It has two children and needs its predicate to use both tuple_idx = 0 and tuple_idx = 1.
This is why the RewriteExpressionForJoin function exists: to translate the Filter's single-source (tuple_idx=0) view of
the world into the Join's dual-source (0 and 1) view.
*/

/*
  Step 1: OptimizeMergeFilterNLJ runs FIRST
  Step 2: OptimizeNLJAsHashJoin runs SECOND

  BEFORE OptimizeMergeFilterNLJ:
  Filter { predicate=(t4.x=t5.x) AND (t6.y=t5.y) AND ... }    ← Separate Filter node
    NLJ { predicate=true }                                      ← Top NLJ
      NLJ { predicate=true }
        Scan t4
        Scan t5
      Scan t6

AFTER OptimizeMergeFilterNLJ:
  NLJ { predicate=(t4.x=t5.x) AND (t6.y=t5.y) AND ... } ← Filter ABSORBED into NLJ! . this is where we are operating
    NLJ { predicate=true }
      Scan t4
      Scan t5
    Scan t6

*/
/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for Spring 2025: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractExpressionRef> left_keys{};
  std::vector<AbstractExpressionRef> right_keys{};
  std::vector<AbstractExpressionRef> residual_exprs;

  // since OptimizeNLJAsHashJoin is run after OptimizeMergeFilterNLJ, NLJ created by OptimizeMergeFilterNLJ
  // have tuple_idx=0 as they scanning of different types have tuple_idx=0 (single table)
  auto is_tuple_idx_zero = [](auto &self, const AbstractExpressionRef &expr) {
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(expr.get()); col_expr != nullptr) {
      return col_expr->GetTupleIdx() == 0;
    }

    for (const auto &ele : expr->GetChildren()) {
      if (!self(self, ele)) {
        return false;
      }
    }
    return true;
  };

  auto extract_keys = [&](auto &self, const AbstractExpressionRef &expr) -> void {
    const auto *comp_expr{dynamic_cast<const ComparisonExpression *>(expr.get())};
    const auto *logic_expr{dynamic_cast<const LogicExpression *>(expr.get())};

    // base case
    if (comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
      const AbstractExpressionRef &left_child{comp_expr->GetChildAt(0)};
      const AbstractExpressionRef &right_child{comp_expr->GetChildAt(1)};

      const auto *left_child_ptr{dynamic_cast<const ColumnValueExpression *>(left_child.get())};
      const auto *right_child_ptr{dynamic_cast<const ColumnValueExpression *>(right_child.get())};

      if (left_child_ptr != nullptr && right_child_ptr != nullptr) {
        /*
          NLJ uses
              EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)
              → needs tuple_idx to pick between the two tuples (0=left, 1=right)

          Hash Join uses
              Evaluate(single_tuple, schema)
              → only one tuple at a time, so tuple_idx must be 0
        */
        if (left_child_ptr->GetTupleIdx() == 0 && right_child_ptr->GetTupleIdx() == 1) {
          left_keys.push_back(left_child);
          right_keys.push_back(std::make_shared<ColumnValueExpression>(0, right_child_ptr->GetColIdx(),
                                                                       right_child_ptr->GetReturnType()));
          return;
        } else if (left_child_ptr->GetTupleIdx() == 1 && right_child_ptr->GetTupleIdx() == 0) {
          left_keys.push_back(right_child);
          right_keys.push_back(
              std::make_shared<ColumnValueExpression>(0, left_child_ptr->GetColIdx(), left_child_ptr->GetReturnType()));
          return;
        }
      }
      return residual_exprs.push_back(expr);

    } else if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
      self(self, logic_expr->GetChildAt(0));
      self(self, logic_expr->GetChildAt(1));
      return;
    }
    residual_exprs.push_back(expr);
  };

  auto optimized_plan = plan;

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const NestedLoopJoinPlanNode *nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(optimized_plan.get());
    extract_keys(extract_keys, nlj_plan->Predicate());

    std::vector<AbstractExpressionRef> left_filter_expr;
    std::vector<AbstractExpressionRef> new_residual_expr;

    for (const auto &expr : residual_exprs) {
      if (is_tuple_idx_zero(is_tuple_idx_zero, expr)) {
        left_filter_expr.push_back(expr);
      } else {
        new_residual_expr.push_back(expr);
      }
    }
    auto left_child = nlj_plan->GetLeftPlan();

    if (!left_filter_expr.empty() && left_child->GetType() == PlanType::NestedLoopJoin) {
      const auto *left_nlj = dynamic_cast<const NestedLoopJoinPlanNode *>(left_child.get());

      AbstractExpressionRef merged_filter = left_filter_expr[0];

      for (size_t i{1}; i < left_filter_expr.size(); i++) {
        merged_filter = std::make_shared<LogicExpression>(merged_filter, left_filter_expr[i], LogicType::And);
      }

      auto rewritten_filter =
          RewriteExpressionForJoin(merged_filter, left_nlj->GetLeftPlan()->OutputSchema().GetColumnCount(),
                                   left_nlj->GetRightPlan()->OutputSchema().GetColumnCount());

      AbstractExpressionRef final_pred{rewritten_filter};

      // this because if left_nlj->Predicate() is some other condition than being true
      // so we can push the current filter condition
      /*
                    [ AND ]
                   /       \
            final_pred     left_nlj->Predicate()
            (t4.x=t5.x)           (t4.z > 100)

      */
      if (!IsPredicateTrue(left_nlj->Predicate())) {
        final_pred = std::make_shared<LogicExpression>(final_pred, left_nlj->Predicate(), LogicType::And);
      }

      // this change the original left child by pushing the filter predicate into it
      // when we traverse do bottom we will operate on it
      left_child =
          std::make_shared<NestedLoopJoinPlanNode>(left_nlj->output_schema_, left_nlj->GetLeftPlan(),
                                                   left_nlj->GetRightPlan(), final_pred, left_nlj->GetJoinType());
    }

    // Convert the current NLJ to a HashJoin if we found equi-join keys
    if (!left_keys.empty() && !right_keys.empty()) {
      optimized_plan =
          std::make_shared<HashJoinPlanNode>(nlj_plan->output_schema_, left_child, nlj_plan->GetRightPlan(),
                                             std::move(left_keys), std::move(right_keys), nlj_plan->GetJoinType());

      if (!new_residual_expr.empty()) {
        AbstractExpressionRef final_filter = new_residual_expr[0];

        for (size_t i{1}; i < new_residual_expr.size(); i++) {
          final_filter = std::make_shared<LogicExpression>(final_filter, new_residual_expr[i], LogicType::And);
        }

        optimized_plan =
            std::make_shared<FilterPlanNode>(nlj_plan->output_schema_, std::move(final_filter), optimized_plan);
      }
    } else {
      // no equi-join keys found, rebuild NLJ with the (possibly updated) left_child
      optimized_plan =
          std::make_shared<NestedLoopJoinPlanNode>(nlj_plan->output_schema_, left_child, nlj_plan->GetRightPlan(),
                                                   nlj_plan->Predicate(), nlj_plan->GetJoinType());
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  return optimized_plan->CloneWithChildren(std::move(children));
}

}  // namespace bustub
