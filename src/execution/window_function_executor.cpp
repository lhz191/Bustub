#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

// void WindowFunctionExecutor::Init()
// {
//     child_executor_->Init();
// }

// auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool 
// {
//  std::vector<Tuple> tuples;
//   std::vector<RID> rids;
//   Tuple child_tuple;
//   RID child_rid;
//   // Step 1: Fetch tuples from child executor and sort them based on ORDER BY
//   while (child_executor_->Next(&child_tuple, &child_rid)) {
//     tuples.emplace_back(child_tuple);
//     rids.emplace_back(child_rid);
//   }
//    if (tuples.empty()) {
//     return false;
//   }
//   std::cout<< plan_->window_func_indexes.size()<<std::endl;
//   std::cout<<plan_->order_bys.size()<<std::endl;
//   std::cout<<plan_->order_bys[0].size()<<std::endl;//juedingla yaobuyao jinxing order
//   std::cout<<plan_->order_bys[0][0].second->ToString()<<std::endl;//+1shi yao order de lie
//   if(plan_->order_bys[0].size()==0)
//   {
    
//   }
//   else
//   {
//      // 根据 SortPlanNode 中指定的 order_bys 对元组进行排序
//     std::sort(tuples.begin(), tuples.end(), [this](const Tuple& t1, const Tuple& t2) {
//         auto schema = child_executor_->GetOutputSchema();
//         std::cout<<"1"<<std::endl;
//                 if (static_cast<bool>(plan_->order_bys[0][0].second->Evaluate(&t1, schema)
//                                         .CompareLessThan(plan_->order_bys[0][0].second->Evaluate(&t2, schema)))) {
//                 return true;
//               } else if (static_cast<bool>(plan_->order_bys[0][0].second->Evaluate(&t1, schema)
//                                         .CompareGreaterThan(plan_->order_bys[0][0].second->Evaluate(&t2, schema)))) {
//                 return false;
//               }
//               return true;
//         });
//         return true;
//     //对于有orderby为每个windows_functions遍历排序后的tuple，然后修改输出的空表里面的值。
// //详细记录一下遍历的方案，不同的tuple可能属于不同的partition，在更新某个partition的max、min、rank的时候，我不仅需要知道当前访问的tuple，我还需要知道属于当前partition的上一个被访问的tuple，以及上一个tuple对应的max、min、rank。所以要用一个map来保存这些信息。
    

//   }

// return false;

// }
auto sort1(const std::vector<std::pair<OrderByType, AbstractExpressionRef>>& order_bys_, const Schema& output_schema_) {
    return [order_bys_, output_schema_](const Tuple& t1, const Tuple& t2) {
        // 比较逻辑
        bool result = false;
        auto schema = output_schema_;
        for (const auto& [order_by_type, order_by_expr] : order_bys_) {
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
        return result;
    };
}
void WindowFunctionExecutor::Init() { 
    this->child_executor_->Init();
    const auto schema = this->child_executor_->GetOutputSchema();

    Tuple child_tuple;
    RID child_rid;
    // Step 1: Fetch tuples from child executor and sort them based on ORDER BY
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        tuples.emplace_back(child_tuple);
    }
    if (tuples.empty()) {
        return;
    }
    //根据函数信息提取所在的列
        std::unordered_set<uint32_t> func_column_idx_set;
        for (auto &[func_column_idx, window_function] : this->plan_->window_functions_) {
            func_column_idx_set.insert(func_column_idx);
        }

        for (auto &[func_column_idx, window_function] : this->plan_->window_functions_) {
            //对于每一个列
            std::cout<<"func_column_idx"<<func_column_idx<<std::endl;
            // Collect partition_by and order_by for this window func
            std::vector<std::pair<OrderByType, AbstractExpressionRef>> total_orders{};
            std::vector<std::pair<OrderByType, AbstractExpressionRef>> partition_orders{};

            for (const auto &partition_by : window_function.partition_by_) {
                // std::cout<<"partition_by"<<partition_by->ToString()<<std::endl;
                // total_orders.emplace_back(OrderByType::ASC, partition_by);
                //huoqu其分区的索引列
                partition_orders.emplace_back(OrderByType::ASC, partition_by);
            }
            for (const auto &order_by : window_function.order_by_) {
                //获取其排序的索引列
                total_orders.push_back(order_by);
            }

        //根据排序的索引列进行排序
        std::sort(this->tuples.begin(), this->tuples.end(), sort1(total_orders, schema));

            auto agg_expr = window_function.function_;

            auto iter = this->tuples.begin();
            //进行聚合操作每次都要遍历一遍tuple，并对res tuple进行修改
            while (iter != this->tuples.end()) {//iter每次增加一个分区tuple的数量
                //std::upper_bound 函数用于在已排序的区间内查找大于某个值的第一个元素的位置。
                //使用 std::upper_bound 函数,,,,以及通过分区索引列进行排序找到当前分组的上界
                auto upper_bound_iter = std::upper_bound(iter,this->tuples.end(), *iter,sort1(partition_orders, schema));
                //lower_bound xiajie
                auto lower_bound_iter = iter;
                //Check window function type and Make default val 
                //直接搬聚合算子的实现
                AggregationType agg_type;
                Value default_value;
                bool is_simple_aggregate_{true};
                switch (window_function.type_) {
                    case WindowFunctionType::CountStarAggregate:
                        agg_type = AggregationType::CountStarAggregate;
                        default_value = ValueFactory::GetIntegerValue(0);
                        break;
                    case WindowFunctionType::CountAggregate:
                        agg_type = AggregationType::CountAggregate;
                        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
                        break;
                    case WindowFunctionType::SumAggregate:
                        agg_type = AggregationType::SumAggregate;
                        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
                        break;
                    case WindowFunctionType::MinAggregate:
                        agg_type = AggregationType::MinAggregate;
                        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
                        break;
                    case WindowFunctionType::MaxAggregate:
                        agg_type = AggregationType::MaxAggregate;
                        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
                        break;
                    case WindowFunctionType::Rank:
                        //import!!!因为窗口函数增加了rank聚合，所以要标志是否进行rank
                        is_simple_aggregate_ = false;
                        default_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
                        break;
                }
               auto group_bys = window_function.partition_by_;
               //buxuyao rank!
                if (is_simple_aggregate_) {
                    const auto agg_exprs = std::vector<AbstractExpressionRef>{agg_expr};
                    const auto agg_types = std::vector<AggregationType>{agg_type};
                    SimpleAggregationHashTable aht{agg_exprs, agg_types};
                    aht.Clear();
                    AggregateKey agg_key = MakeAggregateKey(&(*lower_bound_iter), group_bys);
                    //不需要排序时，这时全局聚合
                    //遍历从 lower_bound_iter 到 upper_bound_iter 之间的所有元素，为每个元素计算聚合值，并将其插入到 SimpleAggregationHashTable 中。
                    if (window_function.order_by_.empty()) {
                        for (auto it = lower_bound_iter; it != upper_bound_iter; ++it) {
                            auto agg_value = MakeAggregateValue(&(*it), agg_expr);
                            aht.InsertCombine(agg_key, agg_value);
                        }
                    }
                    /* Aggregation loop */
                    for (auto it = lower_bound_iter; it != upper_bound_iter; ++it) {
                        auto result_tuple_it = this->result_tuples.begin() + std::distance(this->tuples.begin(), it);
                        if (!window_function.order_by_.empty()) {
                            auto agg_value_inner = MakeAggregateValue(&(*it), agg_expr);
                            aht.InsertCombine(agg_key, agg_value_inner);
                        }

                        /* Start */
                        std::vector<Value> values{};
                        for (uint32_t idx = 0; idx < this->GetOutputSchema().GetColumns().size(); idx++) {
                           //If it is this window function column, the value is the aggregation value
                            if (idx == func_column_idx) 
                            {
                                values.push_back(aht.Begin().Val().aggregates_.begin()[0]);

                            } 
                            //If it is not a window function column, the value should be from the child tuple
                            else if (func_column_idx_set.count(idx) == 0) 
                            {
                                values.push_back(this->plan_->columns_[idx]->Evaluate(&(*it), schema));

                            } 
                            //If it is another window function column, keep it as is
                            else if (result_tuple_it < this->result_tuples.end())
                            {
                                values.push_back(result_tuple_it->GetValue(&this->GetOutputSchema(), idx));

                            } 
                            else 
                            {
                                values.push_back(default_value);
                            }
                        }
                        /* END */

                        if (result_tuple_it < this->result_tuples.end()) {
                            *result_tuple_it = Tuple{values, &this->GetOutputSchema()};
                        } else 
                        {
                            this->result_tuples.emplace_back(Tuple{values, &this->GetOutputSchema()});
                        }
                    }
                } else {
                    /* Case 2: NOT simple aggregation */
                    int global_rank = 0;
                    int local_rank = 0;
                for (auto it = lower_bound_iter; it != upper_bound_iter; ++it) {
                auto result_tuple_it = this->result_tuples.begin() + std::distance(this->tuples.begin(), it);
                        std::vector<Value> values{};
                        for (uint32_t idx = 0; idx < this->GetOutputSchema().GetColumns().size(); idx++) {
                            if (idx == func_column_idx) {
                                ++global_rank;
                                if (local_rank == 0 || !this->Equal(*it, *(it - 1),window_function.order_by_)) local_rank = global_rank;
                                values.push_back(ValueFactory::GetIntegerValue(local_rank));
                            } 
                            else if (func_column_idx_set.count(idx) == 0) 
                            {
                                values.push_back(this->plan_->columns_[idx]->Evaluate(&(*it), schema));
                            }
                            else if (result_tuple_it < this->result_tuples.end()) 
                            {
                                values.push_back(result_tuple_it->GetValue( &this->GetOutputSchema(), idx));
                            } 
                            else 
                            {
                                values.push_back(default_value);
                            }
                        }

                        if (result_tuple_it < this->result_tuples.end()) 
                        {
                            *result_tuple_it = Tuple{values, &this->GetOutputSchema()};
                        } 
                        else 
                        {
                            this->result_tuples.emplace_back(Tuple{values, &this->GetOutputSchema()});
                        }
                    }
                }
                iter = upper_bound_iter;
            }
        }

        this->it_ = this->result_tuples.begin();
}   


auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (it_ == this->result_tuples.end()) return false;
    *tuple = *it_;
    *rid = tuple->GetRid();
    ++it_;
    return true; 
}
}  // namespace bustub
