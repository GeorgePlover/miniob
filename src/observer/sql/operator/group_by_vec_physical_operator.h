/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions);

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override ;
  RC close() override ;

private:
  
  /**
   * @brief 聚合出来的一组数据
   * @details
   * 第一个参数是聚合函数列表，比如需要计算 sum(a), avg(b), count(c)。
   * 第二个参数是聚合的最终结果，它也包含两个元素，第一个是缓存下来的元组，第二个是聚合函数计算的结果。
   * 第二个参数中，之所以要缓存下来一个元组，是要解决这个问题：
   * select a, b, sum(a) from t group by a;
   * 我们需要知道b的值是什么，虽然它不确定。
   */
  //group by 后面的表达式列表
  std::vector<std::unique_ptr<Expression>> group_by_expressions_;
  // select 后面的聚合表达式列表
  std::vector<Expression *> aggregate_expressions_;
  // select 后面的聚合表达式括号内元素列表
  std::vector<Expression *> value_expressions_;
  Chunk chunk_;
  Chunk output_chunck_;
  StandardAggregateHashTable* group_by_hash_table_;
  StandardAggregateHashTable::Scanner* scanner_;
private:
  
};