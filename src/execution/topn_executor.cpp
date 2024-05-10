#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),  plan_(plan),  child_executor_(std::move(child_executor)),
      top_entries_(Comparer(plan_))
    {

    }

void TopNExecutor::Init()
{
     // 初始化子执行器
    child_executor_->Init();

    // 从子执行器获取元组,并将其插入到 top_n_heap_ 中
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        // 根据 order_by 表达式的值,将元组插入到 top_n_heap_ 中
        top_entries_.emplace(std::move(tuple));
        // 如果 top_n_heap_ 的大小超过了 limit,则删除最小的元组
        if (top_entries_.size() > plan_->GetN()) {
            top_entries_.pop();
        }
    }

    // 将 top_n_heap_ 中的元组拷贝到 res_ 中
    res_.reserve(top_entries_.size());
    while (!top_entries_.empty()) {
        res_.emplace_back(std::move(top_entries_.top()));
        top_entries_.pop();
    }

    // 按照 order_by 表达式的值对 res_ 进行排序
    std::sort(res_.begin(), res_.end(), Comparer(plan_));
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
   // 如果 res_ 中还有元组,则返回下一个元组
    if (!res_.empty()) {
        *tuple = res_.front();
        *rid = tuple->GetRid();
        res_.erase(res_.begin());
        return true;
    }

    // 否则,说明没有更多元组了
    return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t
{ 
    return top_entries_.size(); 
}

}  // namespace bustub
