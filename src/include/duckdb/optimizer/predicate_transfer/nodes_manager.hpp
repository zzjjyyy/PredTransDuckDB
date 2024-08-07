#pragma once

#include "duckdb/main/client_context.hpp"

namespace duckdb {
class NodesManager {
public:
	explicit NodesManager(ClientContext &context) : context(context) {
	}

    idx_t NumNodes();

    void AddNode(LogicalOperator *op);

	void SortNodes();

	const vector<RelationStats> GetRelationStats();

	LogicalOperator* getNode(idx_t table_binding) {
		auto itr = nodes.find(table_binding);
		if(itr == nodes.end()) {
			return nullptr;
			throw InternalException("table binding is not found!");
		} else {
			return itr->second;
		}
	}

	unordered_map<idx_t, LogicalOperator*>& getNodes() {
		return nodes;
	}

	vector<LogicalOperator*>& getSortedNodes() {
		return sort_nodes;
	}
	
	void ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &filter_operators);

private:
	ClientContext &context;

	unordered_map<idx_t, LogicalOperator*> nodes;

	//! sorted
	vector<LogicalOperator*> sort_nodes;

	static int nodesCmp(LogicalOperator *a, LogicalOperator *b);
};
}