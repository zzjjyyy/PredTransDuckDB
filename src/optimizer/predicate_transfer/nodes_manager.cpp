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
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <pthread.h>

namespace duckdb {
idx_t NodesManager::NumNodes() {
    return nodes.size();
}

idx_t NodesManager::GetTableIndexinFilter(LogicalOperator *op) {
	if (op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		return op->children[0]->Cast<LogicalGet>().GetTableIndex()[0];
	} else if (op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// For In-Clause optimization
		return op->children[0]->children[0]->Cast<LogicalGet>().GetTableIndex()[0];
	} else if (op->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return op->children[0]->Cast<LogicalAggregate>().GetTableIndex()[0];
	} else {
		return op->Cast<LogicalGet>().GetTableIndex()[0];
	}
}

ColumnBinding NodesManager::FindRename(ColumnBinding col) {
	auto itr = rename_cols.find(col);
	ColumnBinding res = col;
	if (itr != rename_cols.end()) {
		res = itr->second;
		for (auto cur_itr = rename_cols.find(res); cur_itr != rename_cols.end(); cur_itr = rename_cols.find(res)) {
			res = cur_itr->second;
		}
	}
	return res;
}

void NodesManager::AddNode(LogicalOperator *op) {
	op->estimated_cardinality = op->EstimateCardinality(context);
	switch(op->type) {
		case LogicalOperatorType::LOGICAL_GET:
		case LogicalOperatorType::LOGICAL_DELIM_GET:
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT: {
			auto id = op->GetTableIndex()[0];
			if (nodes.find(id) == nodes.end()) {
				nodes[id] = op;
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto id = GetTableIndexinFilter(op);
			if (nodes.find(id) == nodes.end()) {
				nodes[id] = op;
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto id = op->GetTableIndex()[1];
			if (nodes.find(id) == nodes.end()) {
				nodes[id] = op;
			}
			break;
		}
		default: {
			break;
		}
	}
    return;
}

void NodesManager::SortNodes() {
    for(auto &node : nodes) {
		sort_nodes.emplace_back(node.second);
	}
	sort(sort_nodes.begin(), sort_nodes.end(), NodesManager::nodesCmp);
}

void NodesManager::ReSortNodes() {
	sort_nodes.clear();
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

void NodesManager::EraseNode(idx_t key) {
	auto itr = nodes.erase(key);
}

bool can_add_mark = true;

/* Extract All the vertex nodes */
void NodesManager::ExtractNodes(LogicalOperator &plan, vector<reference<LogicalOperator>> &filter_operators) {
    LogicalOperator *op = &plan;
	vector<reference<LogicalOperator>> datasource_filters;
    while (op->children.size() == 1 && !OperatorNeedsRelation(op->type)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
				AddNode(op);
				return;
			}
			else if (op->expressions[0]->type == ExpressionType::OPERATOR_NOT
			&& op->expressions[0]->expression_class == ExpressionClass::BOUND_OPERATOR
			&& op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				can_add_mark = false;
				ExtractNodes(*op->children[0], filter_operators);
				return;
			}
			else {
				can_add_mark = true;
				ExtractNodes(*op->children[0], filter_operators);
				return;
			}
		}
		op = op->children[0].get();
	}

	// bool join_connected = false;
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER
		|| join.join_type == JoinType::LEFT
		|| join.join_type == JoinType::RIGHT
		|| join.join_type == JoinType::SEMI
		|| join.join_type == JoinType::RIGHT_SEMI) {
			for(auto &jc : join.conditions) {
				if(jc.comparison == ExpressionType::COMPARE_EQUAL
				&& jc.left->type == ExpressionType::BOUND_COLUMN_REF
				&& jc.right->type == ExpressionType::BOUND_COLUMN_REF) {
					filter_operators.push_back(*op);
					break;
				}
			}
		} else if (join.join_type == JoinType::MARK && can_add_mark) {
			for(auto &jc : join.conditions) {
				if(jc.comparison == ExpressionType::COMPARE_EQUAL
				&& jc.left->type == ExpressionType::BOUND_COLUMN_REF
				&& jc.right->type == ExpressionType::BOUND_COLUMN_REF) {
					filter_operators.push_back(*op);
					break;
				}
			}
		}
		if (!can_add_mark) {
			can_add_mark = true;
		}
	}
    
    switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// TODO: Transfer Predicate through Group by columns
		LogicalAggregate &agg = op->Cast<LogicalAggregate>();
		if (agg.groups.empty() && agg.grouping_sets.size() <= 1) {
			// optimize children
			RelationStats child_stats;
			ExtractNodes(*op->children[0], filter_operators);
			AddNode(op);
		} else {
			for (int i = 0; i < agg.groups.size(); i++) {
				auto &expr = agg.groups[i];
				if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &colref = expr->Cast<BoundColumnRefExpression>();
					rename_cols.insert(std::make_pair(agg.GetColumnBindings()[i], colref.binding));
				}
			}
			ExtractNodes(*op->children[0], filter_operators);
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		AddNode(op);
		ExtractNodes(*op->children[0], filter_operators);
		return;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN: {
		ExtractNodes(*op->children[0], filter_operators);
		ExtractNodes(*op->children[1], filter_operators);
		return;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		AddNode(op);
		return;
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
		for (int i = 0; i < op->expressions.size(); i++) {
			auto &expr = op->expressions[i];
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &colref = expr->Cast<BoundColumnRefExpression>();
				rename_cols.insert(std::make_pair(op->GetColumnBindings()[i], colref.binding));
			}
		}
		ExtractNodes(*op->children[0], filter_operators);
		return;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		AddNode(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		AddNode(op);
		ExtractNodes(*op->children[0], filter_operators);
		ExtractNodes(*op->children[1], filter_operators);
		return;
	}
	default:
		for (int i = 0; i < op->children.size(); i++) {
			ExtractNodes(*op->children[i], filter_operators);
		}
		return;
	}
}

int NodesManager::nodesCmp(LogicalOperator *a, LogicalOperator *b) {
    return a->estimated_cardinality < b->estimated_cardinality;
}
}