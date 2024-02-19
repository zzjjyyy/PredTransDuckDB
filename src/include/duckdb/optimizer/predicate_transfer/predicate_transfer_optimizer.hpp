#pragma once

#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"

namespace duckdb {
class PredicateTransferOptimizer {
public:
    explicit PredicateTransferOptimizer(ClientContext &context) : context(context), dag_manager(context) {
	}

    unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats = nullptr);

    static LogicalGet& LogicalGetinFilter(LogicalOperator &op);

private:
    int table_num = 0;
    
	ClientContext &context;

    DAGManager dag_manager;

    void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression*> &expressions);

    vector<pair<BlockedBloomFilter*, BlockedBloomFilter*>> CreateBloomFilter(LogicalOperator &node, bool reverse);
};
}