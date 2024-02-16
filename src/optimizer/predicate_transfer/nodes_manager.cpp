#include "duckdb/optimizer/predicate_transfer/nodes_manager.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
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

void NodesManager::AddNode(LogicalOperator *op, const RelationStats &stats) {
	if(op->type == LogicalOperatorType::LOGICAL_GET) {
		op->has_estimated_cardinality = true;
		op->estimated_cardinality = stats.cardinality;
		nodes[op->GetTableIndex()[0]] = op;
	} else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto children = op->children[0].get();
		children->has_estimated_cardinality = true;
		children->estimated_cardinality = stats.cardinality;
		op->has_estimated_cardinality = true;
		op->estimated_cardinality = 0.1 * stats.cardinality;
		nodes[children->GetTableIndex()[0]] = op;
	}
    return;
}

void NodesManager::SortNodes() {
    for(auto &node : nodes) {
		sort_nodes.emplace_back(std::move(node.second));
	}
	sort(sort_nodes.begin(), sort_nodes.end(), NodesManager::nodesCmp);
    int idx = 0;
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
			if (HasNonReorderableChild(*op)) {
				datasource_filters.push_back(*op);
			}
			auto &get = op->children[0]->Cast<LogicalGet>();
			auto stats = RelationStatisticsHelper::ExtractGetStats(get, context);
			AddNode(op, stats);
			return;
		}
		op = op->children[0].get();
	}
    bool non_reorderable_operation = false;
	if (OperatorIsNonReorderable(op->type)) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			// extract join conditions from inner join
			filter_operators.push_back(*op);
		} else {
			non_reorderable_operation = true;
		}
	}

    if (non_reorderable_operation) {
		vector<RelationStats> children_stats;
		for (auto &child : op->children) {
			auto stats = RelationStats();
			PredicateTransferOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child), &stats);
			children_stats.push_back(stats);
		}

        auto combined_stats = RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(*op, children_stats);
	    if (!datasource_filters.empty()) {
		    combined_stats.cardinality =
			    (idx_t)MaxValue(combined_stats.cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
	    }

        AddNode(op, combined_stats);
        return;
    }
    
    switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// optimize children
		RelationStats child_stats;
		PredicateTransferOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &aggr = op->Cast<LogicalAggregate>();
		auto operator_stats = RelationStatisticsHelper::ExtractAggregationStats(aggr, child_stats);
		if (!datasource_filters.empty()) {
			operator_stats.cardinality *= RelationStatisticsHelper::DEFAULT_SELECTIVITY;
		}
		AddNode(op, operator_stats);
		return;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		RelationStats child_stats;
		PredicateTransferOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &window = op->Cast<LogicalWindow>();
		auto operator_stats = RelationStatisticsHelper::ExtractWindowStats(window, child_stats);
		AddNode(op, operator_stats);
		return;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Adding relations to the current join order optimizer
		ExtractNodes(*op->children[0], filter_operators);
		ExtractNodes(*op->children[1], filter_operators);
		return;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		auto &dummy_scan = op->Cast<LogicalDummyScan>();
		auto stats = RelationStatisticsHelper::ExtractDummyScanStats(dummy_scan, context);
		AddNode(op, stats);
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations.
		// create empty stats for dummy scan or logical expression get
		auto &expression_get = op->Cast<LogicalExpressionGet>();
		auto stats = RelationStatisticsHelper::ExtractExpressionGetStats(expression_get, context);
		AddNode(op, stats);
		return;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// TODO: Get stats from a logical GET
		auto &get = op->Cast<LogicalGet>();
		auto stats = RelationStatisticsHelper::ExtractGetStats(get, context);
		// if there is another logical filter that could not be pushed down into the
		// table scan, apply another selectivity.
		if (!datasource_filters.empty()) {
			stats.cardinality =
			    (idx_t)MaxValue(stats.cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddNode(op, stats);
		return;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto child_stats = RelationStats();
		// optimize the child and copy the stats
		PredicateTransferOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &proj = op->Cast<LogicalProjection>();
		// Projection can create columns so we need to add them here
		auto proj_stats = RelationStatisticsHelper::ExtractProjectionStats(proj, child_stats);
		AddNode(op, proj_stats);
		return;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		// optimize the child and copy the stats
		auto &empty_result = op->Cast<LogicalEmptyResult>();
		// Projection can create columns so we need to add them here
		auto stats = RelationStatisticsHelper::ExtractEmptyResultStats(empty_result);
		AddNode(op, stats);
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