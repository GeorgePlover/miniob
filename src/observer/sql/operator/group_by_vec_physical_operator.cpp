/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/expr/expression_tuple.h"
#include "sql/expr/composite_tuple.h"
#include <algorithm>
#include <string>
#include <vector>

using namespace std;
using namespace common;

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions){
    group_by_expressions_ = std::move(group_by_exprs);
    value_expressions_ = std::move(expressions);
    aggregate_expressions_.reserve(value_expressions_.size());
        ranges::for_each(value_expressions_, [this](Expression *expr) {
        auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
        Expression *child_expr     = aggregate_expr->child().get();
        ASSERT(child_expr != nullptr, "aggregate expression must have a child expression");
        aggregate_expressions_.emplace_back(child_expr);
    });
    group_by_hash_table_ = new StandardAggregateHashTable(aggregate_expressions_);
    scanner_ = new StandardAggregateHashTable::Scanner(group_by_hash_table_);
};

RC GroupByVecPhysicalOperator::open(Trx *trx){
   ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

   PhysicalOperator &child = *children_[0];
   RC                rc    = child.open(trx);
   if (OB_FAIL(rc)) {
     LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
     return rc;
   }

//   ExpressionTuple<Expression *> group_value_expression_tuple(value_expressions_);

//   ValueListTuple group_by_evaluated_tuple;
    map<vector<string>, vector<int>> group_by_map;
    
    for (int i = 0; i < group_by_expressions_.size()+aggregate_expressions_.size(); i++) {
        output_chunck_.add_column(unique_ptr<Column>(new Column), i);
    }
    while (OB_SUCC(rc = child.next(chunk_))) {
       //cout<<chunk_.rows()<<endl;
       
       Chunk groups_chunk, aggrs_chunk;
       vector<unique_ptr<Column>> group_by_columns;
       vector<unique_ptr<Column>> aggregate_value_columns;
       for(int i=0;i<group_by_expressions_.size();i++){
           auto expr = group_by_expressions_[i].get();
           group_by_columns.push_back(unique_ptr<Column>(new Column));
           expr->get_column(chunk_, *group_by_columns.back());
           groups_chunk.add_column(move(group_by_columns.back()),i);
       }
       for(int i=0;i<aggregate_expressions_.size();i++){
           auto expr = aggregate_expressions_[i];
           aggregate_value_columns.push_back(unique_ptr<Column>(new Column));
           expr->get_column(chunk_, *aggregate_value_columns.back());
           aggrs_chunk.add_column(move(aggregate_value_columns.back()),i);
       }
       group_by_hash_table_->add_chunk(groups_chunk, aggrs_chunk);
    }
    scanner_->open_scan();
    
    

    if (rc == RC::RECORD_EOF){
        return RC::SUCCESS;
    }
    else{
        ASSERT (false, "unexpected error");
        return RC::UNIMPLENMENT;
    }
}
RC GroupByVecPhysicalOperator::next(Chunk &chunk){
    RC rc = RC::SUCCESS;
    output_chunck_.reset_data();
    if(OB_SUCC(rc = scanner_->next(output_chunck_))){
        chunk.reference(output_chunck_);
        return RC::SUCCESS;
    }
    if(rc == RC::RECORD_EOF)
        return RC::RECORD_EOF;    
    return RC::UNIMPLENMENT; 
}
RC GroupByVecPhysicalOperator::close(){
    children_[0]->close();
    scanner_->close_scan();
    return RC::SUCCESS;
}
