//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),
              left_executor_(std::move(left_executor)),
              right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init()
{
    // std::cout<<"1111111111111"<<std::endl;
    // 初始化左右子执行器
    left_executor_->Init();
    right_executor_->Init();

    // 从左右子执行器中获取所有元组,存储在左右元组向量中
    Tuple tuple;
    RID rid;
    std::vector<Tuple> left_tuples;
    std::vector<Tuple> right_tuples;
    while (left_executor_->Next(&tuple, &rid)) {
        left_tuples.emplace_back(tuple);
    }
    while (right_executor_->Next(&tuple, &rid)) {
        right_tuples.emplace_back(tuple);
    }
    // 遍历左右元组,执行嵌套循环连接
    Schema left_schema = left_executor_->GetOutputSchema();
    Schema right_schema = right_executor_->GetOutputSchema();
    // if(plan_->predicate_->ToString().compare("true") == 0)
    // {
    //      Tuple left_tuple, right_tuple;
    // RID left_rid, right_rid;

    // while (left_executor_->Next(&left_tuple, &left_rid)) {
    //     while (right_executor_->Next(&right_tuple, &right_rid)) {
    //         // 将左右元组的值追加到结果集
    //         std::vector<Value> values;
    //         for (size_t i = 0; i < left_schema.columns_.size(); i++) {
    //             values.emplace_back(left_tuple.GetValue(&left_schema, i));
    //         }
    //         for (size_t i = 0; i < right_schema.columns_.size(); i++) {
    //             values.emplace_back(right_tuple.GetValue(&right_schema, i));
    //         }
    //         results_.emplace(values, &plan_->OutputSchema());
    //     }
    // }
    // }
//    else{
    for (auto &left_tuple: left_tuples) {
        // 重新初始化右子执行器
        right_executor_->Init();

        // 标记当前左元组是否需要进行左外连接
        bool need_left_join = true;

           for(auto &tuple: right_tuples){
        // while (right_executor_->Next(&tuple, &rid)) {
            // 评估连接条件,如果满足则不需要进行左外连接
            Value join_result = plan_->predicate_->EvaluateJoin(&left_tuple, left_schema, &tuple, right_schema);
            if (!join_result.IsNull() && join_result.GetAs<bool>()) {
                // std::cout<<"_______init________"<<std::endl;
                // std::cout<<"this"<<plan_->predicate_->ToString()<<std::endl;
                need_left_join = false;
                // if(plan_->predicate_->ToString()=='true')
                // {
                    
                // }
                // else{
                // 将左右元组的值追加到结果集
                // std::cout<<"this"<<plan_->predicate_->ToString()<<std::endl;
                std::vector<Value> values;
                for (size_t i = 0; i < left_schema.columns_.size(); i++) {
                    values.emplace_back(left_tuple.GetValue(&left_schema, i));
                }
                //  std::cout<<"left_tuple"<<left_tuple.ToString(&left_schema)<<std::endl;
                for (size_t i = 0; i < right_schema.columns_.size(); i++) {
                    values.emplace_back(tuple.GetValue(&right_schema, i));
                }
                // std::cout<<"right_tuple"<<tuple.ToString(&right_schema)<<std::endl;
                // std::cout<<"values[0]"<<values[0].ToString()<<std::endl;
                // std::cout<<"values[1]"<<values[1].ToString()<<std::endl;
                results_.emplace(values, &plan_->OutputSchema());
                    // std::cout<<"    results_.size()"<<results_.size()<<std::endl;
                // }
                // std::cout<<"_______end________"<<std::endl;
            }
        }

        // 如果左元组需要进行左外连接,则将左元组的值追加到结果集,右元组的字段使用null值填充
        if (need_left_join && plan_->GetJoinType() == JoinType::LEFT) {
            std::vector<Value> values;
            for (size_t i = 0; i < left_schema.columns_.size(); i++) {
                values.emplace_back(left_tuple.GetValue(&left_schema, i));
            }
            for (size_t i = 0; i < right_schema.columns_.size(); i++) {
                values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
            }
            results_.emplace(values, &plan_->OutputSchema());
        }
    }
//    }
}
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (!results_.empty()) {
        *tuple = std::move(results_.front());
        results_.pop();
        return true;
    }
    return false;
}

}  // namespace bustub
