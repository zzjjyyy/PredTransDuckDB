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

#ifdef UseHashFilter
    void Add(idx_t create_table, shared_ptr<HashFilter> use_bf, bool reverse);
#else
    void Add(idx_t create_table, shared_ptr<BlockedBloomFilter> use_bf, bool reverse);
#endif

    NodesManager nodes_manager;

    ClientContext &context;

    DAG nodes;
    
private:
    std::mt19937 g;

    vector<LogicalOperator*> ExecOrder;
    
    struct PairHash {
        std::size_t operator()(const pair<int, int>& m) const {
            std::hash<int> hashVal;
            return hashVal(m.first) ^ hashVal(m.second);
        }
    };

    struct PairEqual {
        bool operator()(const pair<int, int>& lhs, const pair<int, int>& rhs) const {
            return lhs.first == rhs.first && lhs.second == rhs.second;
        }
    };

    // (small table, large table), (large table, small table) are both valid
    unordered_map<pair<int, int>, vector<shared_ptr<DAGEdgeInfo>>, PairHash, PairEqual> filters_and_bindings_;
    vector<shared_ptr<DAGEdgeInfo>> selected_filters_and_bindings_;

    void ExtractEdges(LogicalOperator &op,
                      vector<reference<LogicalOperator>> &filter_operators);
    
    void LargestRoot(vector<LogicalOperator*> &sorted_nodes);
    void Small2Large(vector<LogicalOperator*> &sorted_nodes);
    void RandomRoot(vector<LogicalOperator*> &sorted_nodes);

    void CreateDAG();

    pair<int, int> FindEdge(unordered_set<int> &constructed_set, unordered_set<int> &unconstructed_set);

    pair<int, int> FindEdgeRandom(unordered_set<int> &constructed_set, unordered_set<int> &unconstructed_set, std::uniform_int_distribution<idx_t> &dist);

    vector<DAGNode*> GetNeighbors(idx_t node_id);

    void AddEdge(DAGNode &node, vector<DAGEdgeInfo*> &neighbors);

    static int DAGNodesCmp(DAGNode* a, DAGNode* b);
};
}