#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) 
    {

    }

void SortExecutor::Init()
{
    child_executor_->Init();
        // 初始化子执行器

    // 从子执行器获取所有元组,并存储在 sorted_tuples_ 向量中
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        sorted_tuples_.emplace_back(tuple);
    }
    // 根据 SortPlanNode 中指定的 order_bys 对元组进行排序
    std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const Tuple& t1, const Tuple& t2) {
        auto schema = child_executor_->GetOutputSchema();
        for (const auto& [order_by_type, order_by_expr] : plan_->GetOrderBy()) {
            switch (order_by_type){
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
                if (static_cast<bool>(order_by_expr->Evaluate(&t1, schema)
                                        .CompareLessThan(order_by_expr->Evaluate(&t2, schema)))) {
                return true;
              } else if (static_cast<bool>(order_by_expr->Evaluate(&t1, schema)
                                        .CompareGreaterThan(order_by_expr->Evaluate(&t2, schema)))) {
                return false;
              }
              break;
            
         case OrderByType::DESC:
                if (static_cast<bool>(order_by_expr->Evaluate(&t1, schema)
                                        .CompareLessThan(order_by_expr->Evaluate(&t2, schema)))) {
                return false;
              } else if (static_cast<bool>(order_by_expr->Evaluate(&t1, schema)
                                        .CompareGreaterThan(order_by_expr->Evaluate(&t2, schema)))) {
                return true;
              }
              break;

            }
        }
        return false;
    });

    // 将当前位置重置为 0
    current_position_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
        // 检查是否还有更多元组可返回
    if (current_position_ >= sorted_tuples_.size()) {
        return false;
    }

    // 获取下一个元组及其 RID
    *tuple = sorted_tuples_[current_position_];
    current_position_++;

    return true;
}

}  // namespace bustub
