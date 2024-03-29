#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"

namespace duckdb {
bool DAGManager::Build(LogicalOperator &plan) {
    vector<reference<LogicalOperator>> filter_operators;
    nodes_manager.ExtractNodes(plan, filter_operators);
    if(nodes_manager.NumNodes() < 2) {
        return false;
    }
    nodes_manager.SortNodes();
    // extract the edges of the hypergraph, creating a list of filters and their associated bindings.
	ExtractEdges(plan, filter_operators);
	// Create the query_graph hyper edges
	CreateDAG();
    return true;
}

vector<LogicalOperator*>& DAGManager::getSortedOrder() {
    return nodes_manager.getNodes();
}

void DAGManager::Add(ColumnBinding create_table, BlockedBloomFilter *use_bf, bool reverse) {
    if (!reverse) {
        auto in = use_bf->GetCol().table_index;
        nodes.nodes[in]->AddIn(create_table.table_index, use_bf);
    } else {
        auto out = use_bf->GetCol().table_index;
        nodes.nodes[out]->AddOut(create_table.table_index, use_bf);
    }
}

void DAGManager::ExtractEdges(LogicalOperator &op,
                              vector<reference<LogicalOperator>> &filter_operators) {
    auto &sorted_nodes = nodes_manager.getNodes();
	expression_set_t filter_set;
    for (auto &filter_op : filter_operators) {
		auto &f_op = filter_op.get();
        if (f_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
            auto &join = f_op.Cast<LogicalComparisonJoin>();
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
                if(cond.comparison != ExpressionType::COMPARE_EQUAL) {
                    continue;
                }
				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, cond.left->Copy(), cond.right->Copy());
				if (filter_set.find(*comparison) == filter_set.end()) {
					filter_set.insert(*comparison);
					unordered_set<idx_t> left_bindings;
					LogicalJoin::GetExpressionBindings(*comparison->left, left_bindings);
                    unordered_set<idx_t> right_bindings;
					LogicalJoin::GetExpressionBindings(*comparison->right, right_bindings);
                    D_ASSERT(left_bindings.size() == 1 && right_bindings.size() == 1);
                    idx_t left_binding = *left_bindings.begin();
                    idx_t right_binding = *right_bindings.begin();
                    auto left_node = nodes_manager.getNode(left_binding);
                    if(left_node == nullptr) {
                        continue;
                    }
                    auto right_node = nodes_manager.getNode(right_binding);
                    if(right_node == nullptr) {
                        continue;
                    }
                    if (join.join_type == JoinType::INNER) {
                        idx_t left_node_in_order = 0;
                        for (idx_t i = 0; i < sorted_nodes.size(); i++) {
                            if(sorted_nodes[i] == left_node) {
                                left_node_in_order = i;
                                break;
                            }
                        }
                        idx_t right_node_in_order = 0;
                        for (idx_t i = 0; i < sorted_nodes.size(); i++) {
                            if(sorted_nodes[i] == right_node) {
                               right_node_in_order = i;
                               break;
                            }
                        }
                        if (left_node_in_order > right_node_in_order) {
                            auto filter_info = make_uniq<DAGEdgeInfo>(std::move(comparison), *left_node, *right_node);
                            filters_and_bindings_.push_back(std::move(filter_info));
                        } else {
                            auto filter_info = make_uniq<DAGEdgeInfo>(std::move(comparison), *right_node, *left_node);
                            filters_and_bindings_.push_back(std::move(filter_info));
                        }
                    } else if (join.join_type == JoinType::LEFT) {
                        if (left_node->estimated_cardinality < right_node->estimated_cardinality) {
                            auto filter_info = make_uniq<DAGEdgeInfo>(std::move(comparison), *right_node, *left_node);
                            filters_and_bindings_.push_back(std::move(filter_info));
                        }
                    } else if (join.join_type == JoinType::RIGHT) {
                        if (left_node->estimated_cardinality >= right_node->estimated_cardinality) {
                            auto filter_info = make_uniq<DAGEdgeInfo>(std::move(comparison), *left_node, *right_node);
                            filters_and_bindings_.push_back(std::move(filter_info));
                        }
                    }
				}
			}
        }
    }
    return;
}

void DAGManager::CreateDAG() {
    for (auto &filter_and_binding : filters_and_bindings_) {
        idx_t in;
        idx_t out;
        if (filter_and_binding->in_.type == LogicalOperatorType::LOGICAL_GET) {
            in = filter_and_binding->in_.GetTableIndex()[0];
        } else if (filter_and_binding->in_.type == LogicalOperatorType::LOGICAL_FILTER) {
            LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(filter_and_binding->in_);
            in = get.GetTableIndex()[0];
        }
        if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_GET) {
            out = filter_and_binding->out_.GetTableIndex()[0];
        } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_FILTER) {
            LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(filter_and_binding->out_);
            out = get.GetTableIndex()[0];
        }
        if (nodes.nodes.find(in) != nodes.nodes.end()) {
            // build in's out edge
            nodes.nodes[in]->AddIn(out, filter_and_binding->filter.get());
        } else {
            auto node = make_uniq<DAGNode>(in);
            nodes.nodes[in] = std::move(node);
            nodes.nodes[in]->AddIn(out, filter_and_binding->filter.get());
        }
        if (nodes.nodes.find(out) != nodes.nodes.end()) {
            // build out's in edge
            nodes.nodes[out]->AddOut(in, filter_and_binding->filter.get());
        } else {
            auto node = make_uniq<DAGNode>(out);
            nodes.nodes[out] = std::move(node);
            nodes.nodes[out]->AddOut(in, filter_and_binding->filter.get());
        }
    }
}
}