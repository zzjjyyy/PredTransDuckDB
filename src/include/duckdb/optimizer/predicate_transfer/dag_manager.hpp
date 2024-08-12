#pragma once

#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class DAGEdgeInfo {
public:
    DAGEdgeInfo(unique_ptr<Expression> filter, LogicalOperator &large, LogicalOperator &small)
	    : filter(std::move(filter)), large_(large), small_(small) {
	}

    LogicalOperator &large_;
    LogicalOperator &small_;
    unique_ptr<Expression> filter;
    bool large_protect = false;
    bool small_protect = false;
};

class DAGManager {
public:
    DAGManager(ClientContext &context) : nodes_manager(context), context(context) {
        // std::random_device rd;
        // auto seed = rd();
        // auto seed = 3997808281; //2905338331 //495870445
        // std::cout << seed << std::endl;
	    // g.seed(seed);
    }

    //! Extract the join relations, optimizing non-reoderable relations when encountered
	bool Build(LogicalOperator &op);

    vector<LogicalOperator*>& getExecOrder();

    void Add(idx_t create_table, shared_ptr<BlockedBloomFilter> use_bf, bool reverse);

    NodesManager nodes_manager;

    ClientContext &context;

    DAG nodes;
    
private:
    std::mt19937 g;

    vector<LogicalOperator*> ExecOrder;
    
    vector<unique_ptr<DAGEdgeInfo>> filters_and_bindings_;

    void ExtractEdges(LogicalOperator &op,
                      vector<reference<LogicalOperator>> &filter_operators);
    
    void LargestFirst(vector<LogicalOperator*> &sorted_nodes);
    void RandomFirst(vector<LogicalOperator*> &sorted_nodes);

    void CreateDAG();

    vector<DAGNode*> GetNeighbors(idx_t node_id);

    void AddEdge(DAGNode &node, vector<DAGEdgeInfo*> &neighbors);

    static int DAGNodesCmp(DAGNode* a, DAGNode* b);
};
}