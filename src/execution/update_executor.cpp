//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
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

void UpdateExecutor::Init()
{
      table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool{
    // gengxin完毕返回 false
    if (changed) {
        return false;
    }
    changed = true;
    int insert_count = 0;
    // 从子执行器获取需要gengxin的元组
    while (child_executor_->Next(child_tuple.get(), child_rid.get())) {
      // exec_ctx_->
        TupleMeta old_meta = table_info->table_->GetTupleMeta(child_tuple->GetRid());
        old_meta.is_deleted_ = true;
                // 更新索引
        if(old_meta.is_deleted_){
          // std::cout<<"yes"<<std::endl;
        }
        table_info->table_->UpdateTupleMeta(old_meta, child_tuple->GetRid());
        // plan_->target_expressions_
        // plan_->
      // std::cout<<"______________________init_________________"<<std::endl;
      std::vector<Value> new_values;
      for (const auto &expr : plan_->target_expressions_) {
          Value new_value = expr->Evaluate(child_tuple.get(), table_info->schema_);
          // std::cout<<"new_value.ToString()"<<new_value.ToString()<<std::endl;
          new_values.push_back(new_value);
      }
       Tuple newtuple=Tuple(new_values, &table_info->schema_);
        std::optional<RID> new_rid = table_info->table_->InsertTuple(*meta, newtuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info->oid_);
        // std::cout<<"new_rid.value()"<<new_rid.value()<<std::endl;
        // std::cout<<"child_rid.value()"<<child_tuple->GetRid()<<std::endl;
        // std::cout<<"______________________end_________________"<<std::endl;
        std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        // std::cout<<index_info.size()<<std::endl;
        for (const auto &index : index_info) {
          // std::cout<<"_______________3_________"<<(index->index_->GetMetadata()->GetKeyAttrs())[0]<<std::endl;
            Tuple key_tuple = newtuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                       index->index_->GetMetadata()->GetKeyAttrs());
            index->index_->InsertEntry(key_tuple, *new_rid, exec_ctx_->GetTransaction());
        }
        insert_count++;
    }

    // 创建一个包含插入行数的元组并返回
    std::vector<Value> result{Value(INTEGER, insert_count)};
    *tuple = Tuple(result,  &GetOutputSchema());
    return true;
}

}  // namespace bustub
