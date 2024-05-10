#include <memory>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
    std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(limit_plan.children_.size() == 1, "limit should have one children");

    if (const auto *sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan.children_[0].get()))
        if(sort_plan != nullptr) {
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan->children_[0], sort_plan->order_bys_,
                                            limit_plan.limit_);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
