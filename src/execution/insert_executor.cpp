//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/exception.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
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

void InsertExecutor::Init() {
    std::cout<<"________________"<<std::endl;
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // 插入完毕返回 false
    if (changed) {
        return false;
    }
    changed = true;
    int insert_count = 0;
    // 从子执行器获取需要插入的元组
    while (child_executor_->Next(child_tuple.get(), child_rid.get())) {
        std::cout<<"cishu"<<insert_count<<std::endl;
        std::optional<RID> new_rid = table_info->table_->InsertTuple(*meta, *child_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info->oid_);
        std::cout<<"mal"<<std::endl;
        table_info->table_->UpdateTupleMeta(*meta,new_rid.value());
        // 更新索引
        std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        // std::cout<<"index_info.size()"<<index_info.size()<<std::endl;
        for (const auto &index : index_info) {
            // std::cout<<"___________________________3_______________________-"<<std::endl;
            Tuple key_tuple = child_tuple->KeyFromTuple(table_info->schema_, index->key_schema_,
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