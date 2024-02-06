#pragma once

#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"

namespace duckdb {
class PredicateTransferOptimizer {
public:
    explicit PredicateTransferOptimizer(ClientContext &context) : context(context), dag_manager(context) {
	}

    unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, optional_ptr<RelationStats> stats = nullptr);

private:
	ClientContext &context;

    DAGManager dag_manager;

    void GetColumnBindingExpression(unique_ptr<Expression> &expr, vector<unique_ptr<BoundColumnRefExpression>> &expressions);

    vector<pair<idx_t, shared_ptr<BloomFilter>>> CreateBloomFilter(LogicalOperator &node, bool reverse);
};
}