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
    table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    // 插入完毕返回 false
    if (changed) {
        return false;
    }
    changed = true;
    std::cout<<"me"<<std::endl;
    int insert_count = 0;
    // 从子执行器获取需要插入的元组
    while (child_executor_->Next(child_tuple.get(), child_rid.get())) {
        std::cout<<"1"<<std::endl;
        // 从 tuple 获取 TupleMeta
        // data = child_tuple->GetData();
        // std::cout << data << std::endl;
        // memcpy(meta, data, sizeof(TupleMeta));
        // 将元组插入到表中
        std::cout<<"8S1"<<std::endl;
        table_info->table_->InsertTuple(*meta, *child_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info->oid_);
        std::cout<<"8S"<<std::endl;
        // 更新索引
        std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        for (const auto &index : index_info) {
            std::cout<<"3"<<std::endl;
            Tuple key_tuple = child_tuple->KeyFromTuple(table_info->schema_, index->key_schema_,
                                                       index->index_->GetMetadata()->GetKeyAttrs());
            index->index_->InsertEntry(key_tuple, *child_rid, exec_ctx_->GetTransaction());
        }
        std::cout<<2<<std::endl;
        insert_count++;
    }

    // 创建一个包含插入行数的元组并返回
    std::vector<Value> result{Value(INTEGER, insert_count)};
    *tuple = Tuple(result,  &GetOutputSchema());
    return true;
}

}  // namespace bustub