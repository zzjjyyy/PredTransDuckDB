#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include <pthread.h>

namespace duckdb {
idx_t NodesManager::NumNodes() {
    return nodes.size();
}

LogicalGet& LogicalGetinFilter(LogicalOperator *op) {
	if (op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		return op->children[0]->Cast<LogicalGet>();
	} else if (op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// For In-Clause optimization
		return op->children[0]->children[0]->Cast<LogicalGet>();
	}
}

void NodesManager::AddNode(LogicalOperator *op) {
	if(op->type == LogicalOperatorType::LOGICAL_GET) {
		nodes[op->GetTableIndex()[0]] = op;
	} else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		LogicalGet &children = LogicalGetinFilter(op);
		nodes[children.GetTableIndex()[0]] = op;
	} else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		nodes[op->GetTableIndex()[0]] = op;
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		nodes[op->GetTableIndex()[0]] = op;
	}
    return;
}

void NodesManager::SortNodes() {
    for(auto &node : nodes) {
		sort_nodes.emplace_back(node.second);
	}
	sort(sort_nodes.begin(), sort_nodes.end(), NodesManager::nodesCmp);
}

static bool OperatorNeedsRelation(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
		return true;
	default:
		return false;
	}
}

static bool OperatorIsNonReorderable(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return true;
	default:
		return false;
	}
}

static bool HasNonReorderableChild(LogicalOperator &op) {
	LogicalOperator *tmp = &op;
	while (tmp->children.size() == 1) {
		if (OperatorNeedsRelation(tmp->type) || OperatorIsNonReorderable(tmp->type)) {
			return true;
		}
		tmp = tmp->children[0].get();
		if (tmp->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = tmp->Cast<LogicalComparisonJoin>();
			if (join.join_type != JoinType::INNER) {
				return true;
			}
		}
	}
	return tmp->children.empty();
}

void NodesManager::ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &filter_operators) {
    LogicalOperator *op = &plan;
	vector<reference<LogicalOperator>> datasource_filters;
    while (op->children.size() == 1 && !OperatorNeedsRelation(op->type)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
				AddNode(op);
				return;
			} else {
				ExtractNodes(*op->children[0], filter_operators);
				return;
			}
		}
		op = op->children[0].get();
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		/*
		if (join.join_type == JoinType::INNER
		|| join.join_type == JoinType::LEFT
		|| join.join_type == JoinType::RIGHT
		|| join.join_type == JoinType::SEMI
		|| join.join_type == JoinType::RIGHT_SEMI
		|| join.join_type == JoinType::MARK)
		*/
		if (join.join_type == JoinType::INNER || join.join_type == JoinType::MARK || join.join_type == JoinType::SEMI || join.join_type == JoinType::RIGHT_SEMI) {
			for(auto &jc : join.conditions) {
				if(jc.comparison == ExpressionType::COMPARE_EQUAL) {
					filter_operators.push_back(*op);
					break;
				}
			}
			
		}
	}
   
    switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		ExtractNodes(*op->children[0], filter_operators);
		return;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		RelationStats child_stats;
		PredicateTransferOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		AddNode(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Adding relations to the current join order optimizer
		ExtractNodes(*op->children[0], filter_operators);
		ExtractNodes(*op->children[1], filter_operators);
		return;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		AddNode(op);
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations.
		// create empty stats for dummy scan or logical expression get
		AddNode(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// TODO: Get stats from a logical GET
		AddNode(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		AddNode(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		RelationStats child_stats;
		PredicateTransferOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		AddNode(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		AddNode(op);
		return;
	}
	default:
		return;
	}
}

int NodesManager::nodesCmp(LogicalOperator *a, LogicalOperator *b) {
    return a->estimated_cardinality < b->estimated_cardinality;
}
}