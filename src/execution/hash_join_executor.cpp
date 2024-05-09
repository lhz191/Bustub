//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    
  }
}

void HashJoinExecutor::Init()
{
 // 初始化左右子执行器
  left_executor_->Init();
  right_executor_->Init();

  // 从左右子执行器中获取所有元组,并存储在哈希表中
  Tuple ltuple;
  Tuple rtuple;
  RID rid;
    // 遍历左右元组,执行嵌套循环连接
    Schema left_schema =  plan_->GetLeftPlan()->OutputSchema();
    Schema right_schema = plan_->GetRightPlan()->OutputSchema();
    // while (left_executor_->Next(&tuple, &rid)) {
    //     auto join_key = plan_->RightJoinKeyExpressions()[0]->Evaluate(&tuple, left_schema);
    //     hashtable_[HashUtil::HashValue(&join_key)].push_back(tuple);
  while (right_executor_->Next(&rtuple, &rid)) {
    // std::cout<<"plan_->RightJoinKeyExpressions().size()"<<plan_->RightJoinKeyExpressions().size()<<std::endl;
    // std::vector<Value> join_keys;
    auto join_key = plan_->RightJoinKeyExpressions()[0]->Evaluate(&rtuple, right_schema);
    // std::cout<<"join_key"<<join_key.ToString()<<std::endl;
    // for(long unsigned int i=1;i<plan_->RightJoinKeyExpressions().size();i++)
    // {
    //   // join_key.Add(join_key);
    //     auto join_key1 = plan_->RightJoinKeyExpressions()[i]->Evaluate(&rtuple, right_schema);
    //     join_keys.push_back(join_key1);
      
    // }
    // std::cout<<"join_keyhou"<<join_key.ToString()<<std::endl;
    hashtable_[HashUtil::HashValue(&join_key)].push_back(rtuple);
  }

  while (left_executor_->Next(&ltuple, &rid)) {
    right_executor_->Init();
    // std::cout<<"______________init______________"<<std::endl;
    // std::cout<<"plan_->LeftJoinKeyExpressions().size()"<<plan_->LeftJoinKeyExpressions().size()<<std::endl;
    auto join_key = plan_->LeftJoinKeyExpressions()[0]->Evaluate(&ltuple, left_schema);
    //  std::cout<<"join_key"<<join_key.ToString()<<std::endl;
    //     std::cout<<"child0"<<plan_->LeftJoinKeyExpressions()[0].get()->ToString()<<std::endl;
      std::vector<Value> join_keys;
    for(long unsigned int i=1;i<plan_->LeftJoinKeyExpressions().size();i++)
    {
      // std::cout<<"child1"<<plan_->LeftJoinKeyExpressions()[1].get()->ToString()<<std::endl;
      // // join_key.Add(join_key);
      // std::cout<<"join_keyhou"<<join_key.ToString()<<std::endl;
      auto join_key1 = plan_->LeftJoinKeyExpressions()[i]->Evaluate(&ltuple, left_schema);
        join_keys.push_back(join_key1);
    }
    if (hashtable_.count(HashUtil::HashValue(&join_key)) > 0) {
      auto right_tuples = hashtable_[HashUtil::HashValue(&join_key)];
      for (const auto &temp_tuple : right_tuples) {
        std::vector<Value> right_join_keys;
        auto right_join_key = plan_->RightJoinKeyExpressions()[0]->Evaluate(&temp_tuple,right_schema);
      for(long unsigned int i=1;i<plan_->RightJoinKeyExpressions().size();i++)
    {
       auto right_join_key1 = plan_->RightJoinKeyExpressions()[i]->Evaluate(&temp_tuple,right_schema);
      right_join_keys.push_back(right_join_key1);
      //  std::cout<<"right_join_keyhou"<<right_join_key.ToString()<<std::endl;
    }
    // std::cout<<"______________end______________"<<std::endl;
    int flag=1;
    for(long unsigned int i=0;i<plan_->RightJoinKeyExpressions().size()-1;i++)
    {
      //  std::cout<<"join_key"<<join_keys[0].ToString()<<std::endl;
      //   std::cout<<"right_join_key"<<right_join_keys[0].ToString()<<std::endl;
      if(!(join_keys[i].ToString()==right_join_keys[i].ToString()))
      {
        flag=0;
        break;
      }
    }
        if (right_join_key.CompareEquals(join_key) == CmpBool::CmpTrue&&flag==1) {
          // std::cout<<"yes"<<std::endl;
          std::vector<Value> values{};
          values.reserve(plan_->GetLeftPlan()->OutputSchema().columns_.size() +
                         plan_->GetRightPlan()->OutputSchema().columns_.size());
          for (uint32_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().columns_.size(); col_idx++) {
            values.push_back(ltuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
          }
          for (uint32_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().columns_.size(); col_idx++) {
            values.push_back(temp_tuple.GetValue(&plan_->GetRightPlan()->OutputSchema(), col_idx));
          }
         results_.emplace(values, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values{};
      values.reserve(plan_->GetLeftPlan()->OutputSchema().columns_.size() +
                     plan_->GetRightPlan()->OutputSchema().columns_.size());
      for (uint32_t col_idx = 0; col_idx < plan_->GetLeftPlan()->OutputSchema().columns_.size(); col_idx++) {
        values.push_back(ltuple.GetValue(&plan_->GetLeftPlan()->OutputSchema(), col_idx));
      }
      for (uint32_t col_idx = 0; col_idx < plan_->GetRightPlan()->OutputSchema().columns_.size(); col_idx++) {
        values.push_back(
            ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(col_idx).GetType()));
      }
      results_.emplace(values, &GetOutputSchema());
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (!results_.empty()) {
        *tuple = std::move(results_.front());
        results_.pop();
        return true;
    }
    return false;

}

}  // namespace bustub
