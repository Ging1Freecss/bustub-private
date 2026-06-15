#include <cstddef>
#include <cstdint>
#include <memory>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/topn_per_group_plan.h"
#include "execution/plans/window_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {
auto Optimizer::OptimizeWindowFilterAsTopNPerGroup(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeWindowFilterAsTopNPerGroup(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  /*
    check if filter expression
    yes -> check child is window expression
    yes -> get filter predicate and check of expression "<" or "<="
    yes -> check window function type is rank
    yes -> if '<' => n-- this change '<' => '<='
  */

  if (optimized_plan->GetType() == PlanType::Filter) {
    auto fiter_ptr = dynamic_cast<const FilterPlanNode *>(optimized_plan.get());
    const auto &window_node = fiter_ptr->GetChildAt(0);

    if (window_node->GetType() == PlanType::Window && fiter_ptr != nullptr) {
      auto comparison_ptr = dynamic_cast<const ComparisonExpression *>(fiter_ptr->GetPredicate().get());
      auto window_ptr = dynamic_cast<const WindowFunctionPlanNode *>(window_node.get());

      if (comparison_ptr->comp_type_ != ComparisonType::LessThan &&
          comparison_ptr->comp_type_ != ComparisonType::LessThanOrEqual) {
        return optimized_plan;
      }

      // important only rank function should be there
      if (window_ptr->window_functions_.size() != 1) {
        return optimized_plan;
      }

      if (comparison_ptr != nullptr && window_ptr != nullptr) {
        auto column_ptr = dynamic_cast<const ColumnValueExpression *>(comparison_ptr->GetChildAt(0).get());
        auto constant_ptr = dynamic_cast<const ConstantValueExpression *>(comparison_ptr->GetChildAt(1).get());

        if (column_ptr != nullptr && constant_ptr != nullptr) {
          uint32_t col_idx = column_ptr->GetColIdx();
          auto window_itr = window_ptr->window_functions_.find(col_idx);

          if (window_itr != window_ptr->window_functions_.end() &&
              window_itr->second.type_ == WindowFunctionType::Rank) {
            int64_t n = 0;
            if (constant_ptr->val_.GetTypeId() == TypeId::INTEGER) {
              n = constant_ptr->val_.GetAs<int32_t>();
            } else if (constant_ptr->val_.GetTypeId() == TypeId::BIGINT) {
              n = constant_ptr->val_.GetAs<int64_t>();
            } else {
              return optimized_plan;
            }

            if (comparison_ptr->comp_type_ == ComparisonType::LessThan) {
              n--;
            }

            if (n >= 0) {
              return std::make_shared<TopNPerGroupPlanNode>(optimized_plan->output_schema_, window_ptr->GetChildPlan(),
                                                            window_itr->second.partition_by_,
                                                            window_itr->second.order_by_, static_cast<size_t>(n));
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}
}  // namespace bustub