//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"
#include "execution/executors/seq_scan_executor.h"
namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  TableInfo *table_info;
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool changed= false;
  std::unique_ptr<Tuple> child_tuple;
  std::unique_ptr<RID> child_rid;
  Tuple next_tuple;
  TupleMeta tuple_meta;
  const char* data;
  std::unique_ptr<TupleMeta> meta;
  std::unique_ptr<TableIterator> table_iterator_;
  IndexInfo * index_info_;
  HashTableIndexForTwoIntegerColumn *htable_;
  std::vector<RID> rids_;
  std::vector<RID>::iterator rid_iter_;
};
}  // namespace bustub
