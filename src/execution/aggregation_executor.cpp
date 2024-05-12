//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
    aht_(plan_->aggregates_, plan_->agg_types_),
    aht_iterator_(aht_.Begin()),
    child_tuple(std::make_unique<Tuple>()),
    child_rid(std::make_unique<RID>())
{
auto results=aht_.GenerateInitialAggregateValue();
// std::cout<<"flag1"<<std::endl;
}

void AggregationExecutor::Init()
{
    auto results=aht_.GenerateInitialAggregateValue();
    // std::cout<<results[0]<<std::endl;
  child_executor_->Init();
  aht_.Clear();
  // std::cout<<"121:"<<plan_->GetAggregates().size()<<std::endl;
  // std::cout<<plan_->GetAggregates().
// if (plan_->GetAggregates().size() == 0) {
//     std::cout<<"flag2"<<std::endl;
//     // aht_.GenerateInitialAggregateValue();
//         return;
//     }
   // 遍历子执行器,将结果插入到哈希表中
  while (child_executor_->Next(child_tuple.get(), child_rid.get())) {
    aht_.InsertCombine(MakeAggregateKey(child_tuple.get()), MakeAggregateValue(child_tuple.get()));
    // std::cout<<child_tuple.get()->ToString(&plan_->OutputSchema())<<std::endl;
  }
    //   std::cout<<"flag4"<<std::endl;

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool
{
    // std::cout<<"next"<<std::endl;
  if (aht_iterator_ == aht_.End()) {
    if(!plan_->GetGroupBys().empty())
    {
      return false;
    }
    // std::cout<<"flag3"<<std::endl;
    if(changed==false){
          // std::cout<<"flag4"<<std::endl;
AggregateValue initial_values = aht_.GenerateInitialAggregateValue();
            *tuple = Tuple{initial_values.aggregates_, &GetOutputSchema()};
            changed=true;
            // aht_.InsertCombine(MakeAggregateKey(tuple), MakeAggregateValue(tuple));
            return true;
    }
    return false;
  }
    // if (plan_->GetAggregates().size() == 0) {
    //     return child_executor_->Next(child_tuple.get(), child_rid.get());
    // }
    // std::cout<<plan_->group_bys_[1]<<std::endl;bu neng shuchu
  std::vector<Value> values;
  changed=true;
//  Value values = aht_iterator_.Val().aggregates_;
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
//   std::cout<<"aht_iterator_.Key()"<<aht_iterator_.Key().operator==<<std::endl;
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
