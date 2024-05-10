//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct Comparer {
  explicit Comparer(const TopNPlanNode *plan) : plan_(plan) {}
  auto operator()(const Tuple &a, const Tuple &b) -> bool {
    for (auto &order_by : plan_->GetOrderBy()) {
      auto a_key = order_by.second->Evaluate(&a, *plan_->output_schema_);
      auto b_key = order_by.second->Evaluate(&b, *plan_->output_schema_);
      if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
        if (a_key.CompareLessThan(b_key) == CmpBool::CmpTrue) {
          return true;
        }
        if (a_key.CompareGreaterThan(b_key) == CmpBool::CmpTrue) {
          return false;
        }
      } else if (order_by.first == OrderByType::DESC) {
        if (a_key.CompareGreaterThan(b_key) == CmpBool::CmpTrue) {
          return true;
        }
        if (a_key.CompareLessThan(b_key) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    return false;
  }

 private:
  const TopNPlanNode *plan_;
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
    /** 用于存储前 N 个元组的优先队列 */
  std::priority_queue<Tuple, std::vector<Tuple>, Comparer> top_entries_;

  std::vector<Tuple> res_;
};
}  // namespace bustub
