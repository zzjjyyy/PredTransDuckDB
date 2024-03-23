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
	ClientContext &context;

    DAGManager dag_manager;

    std::unordered_map<void*, unique_ptr<LogicalOperator>> replace_map_forward;

    std::unordered_map<void*, unique_ptr<LogicalOperator>> replace_map_backward;

    void GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression*> &expressions);

    vector<pair<ColumnBinding, BlockedBloomFilter*>> CreateBloomFilter(LogicalOperator &node, bool reverse);

    idx_t GetNodeId(LogicalOperator &node, vector<LogicalType> &cur_types, vector<idx_t> &col_mapping);

    void GetAllBFUsed(idx_t cur, vector<BlockedBloomFilter*> &temp_result_to_use, vector<idx_t> &depend_nodes, bool reverse);

    void GetAllBFCreate(idx_t cur, unordered_map<idx_t, vector<BlockedBloomFilter*>> &temp_result_to_create, bool reverse);

    unique_ptr<LogicalCreateBF>
    BuildSingleCreateOperator(LogicalOperator &node,
							  unordered_map<idx_t, vector<BlockedBloomFilter*>> &temp_result_to_create);

    unique_ptr<LogicalUseBF>
    BuildUseOperator(LogicalOperator &node,
					 vector<BlockedBloomFilter*> &temp_result_to_use,
                     vector<idx_t> &depend_nodes,
					 bool reverse);

    unique_ptr<LogicalCreateBF>
    BuildCreateUsePair(LogicalOperator &node,
                       vector<BlockedBloomFilter*> &temp_result_to_use,
					   unordered_map<idx_t, vector<BlockedBloomFilter*>> &temp_result_to_create,
                       vector<idx_t> &depend_nodes,
					   bool reverse);
};
}