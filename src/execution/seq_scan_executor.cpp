//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) 
{
    // std::cout<<"p22"<<std::endl;
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    if (table_info == Catalog::NULL_TABLE_INFO) {
        throw std::runtime_error("Table not found in catalog");
    }
    // std::cout<<"p33"<<std::endl;
    // Create a table iterator from the table heap
    table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
}

void SeqScanExecutor::Init()
{
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    if (table_info == Catalog::NULL_TABLE_INFO) {
        throw std::runtime_error("Table not found in catalog");
    }
    // Create a table iterator from the table heap
    table_iterator_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
    // table_iterator_ = table_info->table_->MakeIterator();

}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    while (!table_iterator_->IsEnd()) {
        // 从表迭代器中获取元组
        // std::cout<<"p2"<<std::endl;
        auto [tuple_meta, next_tuple] = table_iterator_->GetTuple();
        // std::cout<<"p3"<<std::endl;
        *tuple = next_tuple;
        *rid = table_iterator_->GetRID();
        // 推进表迭代器到下一个元组
        std::cout<<"p1"<<std::endl;
        ++(*table_iterator_);
        // 如果存在过滤谓词,则应用过滤
        if (plan_->filter_predicate_ == nullptr || plan_->filter_predicate_->Evaluate(tuple, table_info->schema_).GetAs<bool>()) {
            std::cout<<tuple_meta.is_deleted_<<std::endl;
            if(tuple_meta.is_deleted_==false){//jian cha shi fou bei shan
            std::cout<<"p2"<<std::endl;
            // 当前元组满足过滤条件,返回给调用者
            return true;
            }
            else{
                std::cout<<"p3"<<std::endl;
                // return false;
                // return true;
            }
        }
    }

    // 表迭代器已经到达结尾
    return false;
}
}  // namespace bustub
