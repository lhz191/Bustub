//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan),
    index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)}
{
    htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
    table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    if (table_info == Catalog::NULL_TABLE_INFO) {
        throw std::runtime_error("Table not found in catalog");
    }
    // table_iterator_= std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}
void IndexScanExecutor::Init() {
     if (plan_->filter_predicate_ == nullptr)
     {
        return;
     }
    htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
    table_info = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
    if (table_info == Catalog::NULL_TABLE_INFO) {
        throw std::runtime_error("Table not found in catalog");
    }
    // Create a table iterator from the table heap
    // table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());

    // 构建扫描索引的 Key 元组
    std::vector<Value> index_key_values = {};
for (const auto &child_expr : plan_->filter_predicate_->children_) {
    const auto *right_expr = dynamic_cast<const ConstantValueExpression *>(child_expr.get());
    if (right_expr != nullptr) {
        Value v = right_expr->val_;
        index_key_values.push_back(v);
    }
}
        Tuple index_key_tuple(index_key_values, index_info_->index_->GetKeySchema());

        // 扫描索引并获取 RID 列表
    htable_->ScanKey(index_key_tuple, &rids_, exec_ctx_->GetTransaction());
    rid_iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (plan_->filter_predicate_ == nullptr)
     {
        return false;
     }
     if(rids_.size()==0)
     {
        // SeqScanExecutor.Nest(tuple,rid);
        // return true;
        return false;
     }
     while (rid_iter_ != rids_.end()) {
        // 检查当前RID是否被删除
        tuple_meta = table_info->table_->GetTuple(*rid_iter_).first;
        if (!tuple_meta.is_deleted_) {
            // 更新输出参数
            *tuple = table_info->table_->GetTuple(*rid_iter_).second;
            *rid = *rid_iter_;
            rid_iter_++;
            changed = true;
            return true;
        }
        rid_iter_++;
    }
    return false;

}
}  // namespace bustub
