#pragma once

#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {
class PredicateTransferOptimizer {
public:
    explicit PredicateTransferOptimizer(ClientContext &context) : context(context), dag_manager(context) {
	}

    unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats = nullptr);

    static LogicalGet& LogicalGetinFilter(LogicalOperator &op);

    unique_ptr<LogicalOperator> InsertCreateBFOperator(unique_ptr<LogicalOperator> plan);

    unique_ptr<LogicalOperator> InsertCreateBFOperator_d(unique_ptr<LogicalOperator> plan);
private:
    int table_num = 0;
    
	ClientContext &context;

    DAGManager dag_manager;

    std::unordered_map<void*, unique_ptr<LogicalCreateBF>> replace_map_forward;

    std::unordered_map<void*, unique_ptr<LogicalCreateBF>> replace_map_backward;

    void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression*> &expressions);

    vector<pair<ColumnBinding, BlockedBloomFilter*>> CreateBloomFilter(LogicalOperator &node, bool reverse);
};
}