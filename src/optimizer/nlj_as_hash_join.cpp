#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "execution/expressions/logic_expression.h"

namespace bustub {
// 优化单个比较表达式,判断是否可以用于构建哈希表
auto OptimizeSingleExpression(const ComparisonExpression *expr, const NestedLoopJoinPlanNode &nlj_plan,
                              std::vector<AbstractExpressionRef> &left_exprs,
                              std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  // 检查是否为相等比较
  if (expr->comp_type_ == ComparisonType::Equal) {
    // 检查左表达式是否为列值表达式
    if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get())) {
      // 检查右表达式是否为列值表达式
      if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get())) {
        // 创建新的列值表达式,元组索引为0
        auto left_expr_tuple_0 =
            std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
        auto right_expr_tuple_0 =
            std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
        // 检查左右表达式是否分别来自左表和右表
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          // 将左表达式和右表达式添加到相应的向量中
          left_exprs.push_back(std::move(left_expr_tuple_0));
          right_exprs.push_back(std::move(right_expr_tuple_0));
          return true;
        }
        if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
          // 将左表达式和右表达式添加到相应的向量中
          right_exprs.push_back(std::move(left_expr_tuple_0));
          left_exprs.push_back(std::move(right_expr_tuple_0));
          return true;
        }
      }
    }
  }
  // 如果不满足条件,返回false
  return false;
}
void dfs(const ComparisonExpression *left_expr,bustub::AbstractExpression *temp_a,const NestedLoopJoinPlanNode & nlj_plan,std::vector<AbstractExpressionRef>& left_exprs,std::vector<AbstractExpressionRef>& right_exprs)
{
  if(left_expr!=nullptr)
  {
    return;
  }
            if(left_expr==nullptr)
          {
            for(long unsigned int i=0;i<(temp_a->children_.size());i++)
            {
              if(dynamic_cast<const ComparisonExpression*>(temp_a->children_[i].get())==nullptr)
              {
                dfs(dynamic_cast<const ComparisonExpression*>(temp_a->children_[i].get()),temp_a->children_[i].get(), nlj_plan, left_exprs, right_exprs);
              }
              else
              {
      OptimizeSingleExpression(dynamic_cast<const ComparisonExpression *>(temp_a->children_[i].get()), nlj_plan, left_exprs, right_exprs);
              }
            }
          }
}
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
   std::vector<AbstractPlanNodeRef> children;
  //  std::cout<<"palnchild"<<plan->children_.size()<<std::endl;
  for (const auto &child : plan->GetChildren()) {
    // std::cout<<"child"<<child->ToString()<<std::endl;
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // std::cout<<"opchid"<<optimized_plan->children_.size()<<std::endl;
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    // std::cout<<"nlj_plan.children_.size()"<<nlj_plan.children_.size()<<std::endl;
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get())) {
      // std::cout<<"1212:"<<expr->children_.size()<<std::endl;
      if (expr->children_.size() == 2) {
        // std::cout<<"12121223:"<<expr->children_[0].get()->ToString();
        auto left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
        auto right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
        // std::cout<<(right_expr->ToString())<<std::endl;
        if (left_expr != nullptr && right_expr != nullptr) {
          // std::cout<<"????????"<<std::endl;
          if (OptimizeSingleExpression(left_expr, nlj_plan, left_exprs, right_exprs) &&
              OptimizeSingleExpression(right_expr, nlj_plan, left_exprs, right_exprs)) {
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                      std::move(right_exprs), nlj_plan.GetJoinType());
          }
        }
        else if(left_expr==nullptr&&right_expr!=nullptr)
        {
          // for(long unsigned int i=0;i<(expr->children_[0].get())->children_.size();i++)
          // {
          //   if(dynamic_cast<const ComparisonExpression *>((expr->children_[0].get())->children_[i].get())==nullptr)
          //   {
          //     // std::cout<<"yes"<<<<std::endl;
          //     for(long unsigned int j=0;j<((expr->children_[0].get())->children_[i].get())->children_.size();j++)
          //     {
          //       OptimizeSingleExpression(dynamic_cast<const ComparisonExpression *>(((expr->children_[0].get()->children_[i].get())->children_[j]).get()), nlj_plan, left_exprs, right_exprs);
          // }
          //   }
          //   else{
          //   // std::cout<<"yes"<<((expr->children_[0].get())->children_[i]->ToString())<<std::endl;
          //   OptimizeSingleExpression(dynamic_cast<const ComparisonExpression *>((expr->children_[0].get())->children_[i].get()), nlj_plan, left_exprs, right_exprs);
          //   }
          // }
          left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
          auto temp_a =(expr->children_[0].get());
          dfs(left_expr,temp_a,nlj_plan,left_exprs,right_exprs);
          if((OptimizeSingleExpression(right_expr, nlj_plan, left_exprs, right_exprs)))
          {
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
          }
        }
        else if(left_expr!=nullptr&&right_expr==nullptr)
        {
          for(long unsigned int i=0;i<(expr->children_[1].get())->children_.size();i++)
          {
            OptimizeSingleExpression(dynamic_cast<const ComparisonExpression *>((expr->children_[1].get())->children_[i].get()), nlj_plan, left_exprs, right_exprs);
          }
          if((OptimizeSingleExpression(left_expr, nlj_plan, left_exprs, right_exprs)))
          {
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
          }

        }
        else
        {
          for(long unsigned int i=0;i<(expr->children_[0].get())->children_.size();i++)
          {
            OptimizeSingleExpression(dynamic_cast<const ComparisonExpression *>((expr->children_[0].get())->children_[i].get()), nlj_plan, left_exprs, right_exprs);
          }
          for(long unsigned int i=0;i<(expr->children_[1].get())->children_.size();i++)
          {
            OptimizeSingleExpression(dynamic_cast<const ComparisonExpression *>((expr->children_[1].get())->children_[i].get()), nlj_plan, left_exprs, right_exprs);
          }
                        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());

        }
        return optimized_plan;
      }
    } else if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get())) {
      // std::cout<<"flag222"<<std::endl;
      if (OptimizeSingleExpression(expr, nlj_plan, left_exprs, right_exprs)) {
        // std::cout<<"flag222"<<std::endl;
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}



}  // namespace bustub