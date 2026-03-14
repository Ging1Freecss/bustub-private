//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_limit_as_topn.cpp
//
// Identification: src/optimizer/sort_limit_as_topn.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief optimize sort + limit as top N
 */
auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    AbstractPlanNodeRef child = optimized_plan->GetChildAt(0);

    if (child->GetType() == PlanType::Sort) {
      auto child_ptr = dynamic_cast<const SortPlanNode *>(child.get());
      auto plan_ptr = dynamic_cast<const LimitPlanNode *>(optimized_plan.get());
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, child_ptr->GetChildPlan(),
                                            child_ptr->GetOrderBy(), plan_ptr->limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
