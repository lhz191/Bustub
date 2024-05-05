#include "optimizer/optimizer.h"
#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/index_scan_executor.h"
#include "catalog/schema.h"
namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
    // 1. 首先递归地优化子节点
    std::vector<AbstractPlanNodeRef> optimized_children;
    // std::cout<<"------------"<<optimized_plan->children_.size() == 1<<std::endl;
    if(plan->GetChildren().size()>1)
    {
        std::cout<<"------------"<<std::endl;
        return plan;
    }
    for (const auto &child : plan->GetChildren()) {
        optimized_children.emplace_back(OptimizeSeqScanAsIndexScan(child));
    }
    auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));
    // 2. 检查当前节点是否为 SeqScanPlanNode
    const auto *seq_scan = dynamic_cast<const SeqScanPlanNode *>(optimized_plan.get());
    if (seq_scan == nullptr) {
        // 如果不是 SequentialScanPlanNode，直接返回优化后的 plan
        return optimized_plan;
    }
    // 3. 检查是否存在过滤谓词
    if (seq_scan->filter_predicate_ == nullptr) {
        // 没有过滤谓词，无法优化为 IndexScan
        // std::cout<<3<<std::endl;
        return optimized_plan;
    }
  //  std::cout<<(exec_ctx== nullptr)<<std::endl;
    // 4. 检查过滤谓词是否可以利用索引
    table_info = catalog_.GetTable(seq_scan->table_name_);
    std::vector<IndexInfo *> indexes = catalog_.GetTableIndexes(seq_scan->table_name_);
    std::vector<IndexInfo *> usable_index = {};
    // 检查过滤谓词是否为 ColumnValueExpression
    auto *col_value_expr = seq_scan->filter_predicate_.get();
    if(col_value_expr == nullptr)
    {
        return optimized_plan;
    }
    if (col_value_expr != nullptr) {
        // 检查列名是否在索引的键列中
        for (auto index : indexes) {
            Schema key_schema = index->key_schema_;
            // std::cout<<"key_schema.columns_.size()"<<key_schema.columns_.size()<<std::endl;
            for (size_t i = 0; i < key_schema.columns_.size(); i++) {
                // std::cout<<"key_schema.GetColumn(i).GetName()"<<key_schema.GetColumn(i).GetName()<<std::endl;
                // std::cout<<"table_info->schema_.GetColumn(i).GetName()"<<table_info->schema_.GetColumn(i).GetName()<<std::endl;
                // if (key_schema.GetColumn(i).GetName() == table_info->schema_.GetColumn(i).GetName()) {
                    usable_index.push_back(index);
                    break;
                }
            }
        }

    if (usable_index.size() == 0) {
        // 没有合适的索引，无法优化为 IndexScan
        // std::cout<<2<<std::endl;
        return optimized_plan;
    }

    // 5. 创建新的 IndexScanPlanNode
// auto index_scan = std::make_unique<IndexScanPlanNode>(
//     seq_scan->output_schema_, table_info->oid_, usable_index->index_oid_,
//     seq_scan->filter_predicate_);

//     // 6. 将过滤谓词推送到 IndexScanPlanNode
//     index_scan->filter_predicate_ = seq_scan->filter_predicate_;
    auto index_scan = std::make_shared<IndexScanPlanNode>(
        seq_scan->output_schema_, table_info->oid_, usable_index[0]->index_oid_,
        seq_scan->filter_predicate_, nullptr);
    index_scan->filter_predicate_ = seq_scan->filter_predicate_;
    index_scan->children_ = std::move(optimized_children);
// std::cout<<12323<<std::endl;

       // 6. 将 unique_ptr 转换为 shared_ptr 再转换为 AbstractPlanNode
    // return index_scan;
      return std::static_pointer_cast<const AbstractPlanNode>(index_scan);
    // return std::static_pointer_cast<const AbstractPlanNode>(std::make_shared<IndexScanPlanNode>(*index_scan));
}

}  // namespace bustub

