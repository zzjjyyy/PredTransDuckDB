#pragma once

#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DAGEdgeInfo {
public:
    DAGEdgeInfo(unique_ptr<Expression> filter, LogicalOperator &in, LogicalOperator &out)
	    : filter(std::move(filter)), in_(in), out_(out) {
	}

    LogicalOperator &in_;
    LogicalOperator &out_;
    unique_ptr<Expression> filter;
};

class DAGManager {
public:
    DAGManager(ClientContext &context) : nodes_manager(context), context(context) {
    }

    //! Extract the join relations, optimizing non-reoderable relations when encountered
	bool Build(LogicalOperator &op);

    vector<LogicalOperator*>& getSortedOrder();

    void Add(ColumnBinding create_table, shared_ptr<BlockedBloomFilter> use_bf, bool reverse);

    NodesManager nodes_manager;

    ClientContext &context;

    DAG nodes;
    
private:
    vector<unique_ptr<DAGEdgeInfo>> filters_and_bindings_;

    void ExtractEdges(LogicalOperator &op,
                      vector<reference<LogicalOperator>> &filter_operators);
    
    void CreateDAG();
};
}