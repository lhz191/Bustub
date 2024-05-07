//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)),
    child_tuple(std::make_unique<Tuple>()),
    child_rid(std::make_unique<RID>()),
    data(nullptr),
    meta(std::make_unique<TupleMeta>()) {
        table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    }

void DeleteExecutor::Init()
{
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool 
{
       // gengxin完毕返回 false
    if (changed) {
        return false;
    }
    changed = true;
    int insert_count = 0;
    // 从子执行器获取需要gengxin的元组
    while (child_executor_->Next(child_tuple.get(), child_rid.get())) {
      // exec_ctx_->
    //   std::cout<<"________________-init________________"<<std::endl;
        TupleMeta old_meta = table_info->table_->GetTupleMeta(child_tuple->GetRid());
        old_meta.is_deleted_ = true;
                // 更新索引
        if(old_meta.is_deleted_){
        }
        table_info->table_->UpdateTupleMeta(old_meta, child_tuple->GetRid());
        // plan_->target_expressions_
        // plan_->
        std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        for (const auto &index : index_info) {
            Tuple key_tuple = child_tuple->KeyFromTuple(table_info->schema_, index->key_schema_,
                                                       index->index_->GetMetadata()->GetKeyAttrs());
            index->index_->DeleteEntry(key_tuple, *child_rid, exec_ctx_->GetTransaction());
        }
        // std::cout<<"________________-end________________"<<std::endl;
        insert_count++;
    }

    // 创建一个包含插入行数的元组并返回
    std::vector<Value> result{Value(INTEGER, insert_count)};
    *tuple = Tuple(result,  &GetOutputSchema());
    return true;
}

}  // namespace bustub
