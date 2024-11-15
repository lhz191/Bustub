//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), current_index_(0) 
    {

    }

void LimitExecutor::Init()
{
     child_executor_->Init();
     current_index_=0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool 
{
       // Check if reached the limit
    if (current_index_ >= plan_->GetLimit()) {
        return false;
    }

    // Get the next tuple from the child executor
    bool has_next = child_executor_->Next(tuple, rid);
    if (has_next) {
        current_index_++;
    }
    return has_next;
}

}  // namespace bustub
