#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {
class NodesManager {
public:
	explicit NodesManager(ClientContext &context) : context(context) {
	}

    idx_t NumNodes();

    void AddNode(LogicalOperator *op, const RelationStats &stats);

	void SortNodes();

	const vector<RelationStats> GetRelationStats();

	LogicalOperator* getNode(idx_t table_binding) {
		auto itr = nodes.find(table_binding);
		if(itr == nodes.end()) {
			throw InternalException("table binding is ot found!");
		} else {
			return itr->second;
		}
	}

	vector<LogicalOperator*>& getNodes() {
		return sort_nodes;
	}
	
	void ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &filter_operators);

private:
	ClientContext &context;

	unordered_map<idx_t, LogicalOperator*> nodes;

	//! sorted
	vector<LogicalOperator*> sort_nodes;

	static int nodesCmp(LogicalOperator *a, LogicalOperator *b);

	// table index maps to order index
	unordered_map<idx_t, idx_t> nodes_mappings;
};
}