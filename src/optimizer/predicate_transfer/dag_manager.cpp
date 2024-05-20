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
    if(filters_and_bindings_.size() == 0) {
        return false;
    }
	// Create the query_graph hyper edges
	CreateDAG();
    return true;
}

vector<LogicalOperator*>& DAGManager::getExecOrder() {
    // The root as first
    return ExecOrder;
}

void DAGManager::Add(ColumnBinding create_table, shared_ptr<BlockedBloomFilter> use_bf, bool reverse) {
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
    auto &sorted_nodes = nodes_manager.getSortedNodes();
	expression_set_t filter_set;
    for (auto &filter_op : filter_operators) {
		auto &f_op = filter_op.get();
        if (f_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || f_op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
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
                    if (join.join_type == JoinType::INNER
                    || join.join_type == JoinType::SEMI
                    || join.join_type == JoinType::RIGHT_SEMI
                    || join.join_type == JoinType::MARK) {
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
    auto &sorted_nodes = nodes_manager.getSortedNodes();
    // Create Vertices
    for(auto &vertex : nodes_manager.getNodes()) {
        // Set the last operator as root
        if(vertex.second == sorted_nodes.back()) {
            nodes.nodes[vertex.first] = make_uniq<DAGNode>(vertex.first, true);
        } else {
            nodes.nodes[vertex.first] = make_uniq<DAGNode>(vertex.first, false);
        }
    }
    // Add Edge to the vertices
    for (int i = sorted_nodes.size() - 1; i >= 0; i--) {
        auto op = sorted_nodes[i];
        // get the DAG Node from large to small by id index
        idx_t node_id;
        if (op->type == LogicalOperatorType::LOGICAL_GET) {
            node_id = op->GetTableIndex()[0];
        } else if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
            LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(*op);
            node_id = get.GetTableIndex()[0];
        } else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
            node_id = op->GetTableIndex()[0];
        } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
            node_id = op->GetTableIndex()[0];
        }
        auto& node = nodes.nodes[node_id];
        // Get the neighbor edge order by desc size
        auto neighbors = GetNeighbors(node_id, sorted_nodes);
        AddEdge(*node, neighbors);
    }
}

vector<DAGEdgeInfo*> DAGManager::GetNeighbors(idx_t node_id, vector<LogicalOperator*> &sorted_nodes) {
    vector<DAGEdgeInfo*> result;
    for (auto &filter_and_binding : filters_and_bindings_) {
        if(filter_and_binding) {
            if(&filter_and_binding->in_ == nodes_manager.getNode(node_id)) {
                result.emplace_back(filter_and_binding.get());
            }
        }
    }
    sort(result.begin(), result.end(), DAGManager::EdgesCmp);
    return result;
}

void DAGManager::AddEdge(DAGNode &node, vector<DAGEdgeInfo*> &neighbors) {
    if(node.root || (!node.root && node.out_.size() != 0)) {
        for (auto &filter_and_binding : neighbors) {
            idx_t out;
            if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_GET) {
                out = filter_and_binding->out_.GetTableIndex()[0];
            } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_FILTER) {
                LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(filter_and_binding->out_);
                out = get.GetTableIndex()[0];
            } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                out = filter_and_binding->out_.GetTableIndex()[0];
            } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                out = filter_and_binding->out_.GetTableIndex()[0];
            }
            node.AddIn(out, filter_and_binding->filter.get());
            nodes.nodes[out]->AddOut(node.Id(), filter_and_binding->filter.get());
        }
    } else {
        if (neighbors.size() > 0) {
            auto& special_case = neighbors[0];
            idx_t in;
            if (special_case->out_.type == LogicalOperatorType::LOGICAL_GET) {
                in = special_case->out_.GetTableIndex()[0];
            } else if (special_case->out_.type == LogicalOperatorType::LOGICAL_FILTER) {
                LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(special_case->out_);
                in = get.GetTableIndex()[0];
            } else if (special_case->out_.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                in = special_case->out_.GetTableIndex()[0];
            } else if (special_case->out_.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                in = special_case->out_.GetTableIndex()[0];
            }
            node.AddOut(in, special_case->filter.get());
            nodes.nodes[in]->AddIn(node.Id(), special_case->filter.get());
            if(std::find(ExecOrder.begin(), ExecOrder.end(), &special_case->out_) == ExecOrder.end()) {
                ExecOrder.emplace_back(&special_case->out_);
            }
            for(int i = 1; i < neighbors.size(); i++) {
                auto &filter_and_binding = neighbors[i];
                idx_t out;
                if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_GET) {
                    out = filter_and_binding->out_.GetTableIndex()[0];
                } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_FILTER) {
                    LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(filter_and_binding->out_);
                    out = get.GetTableIndex()[0];
                } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                    out = filter_and_binding->out_.GetTableIndex()[0];
                } else if (filter_and_binding->out_.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                    out = filter_and_binding->out_.GetTableIndex()[0];
                }
                node.AddIn(out, filter_and_binding->filter.get());
                nodes.nodes[out]->AddOut(node.Id(), filter_and_binding->filter.get());
            }
        }
    }
    auto op = nodes_manager.getNode(node.Id());
    if(std::find(ExecOrder.begin(), ExecOrder.end(), op) == ExecOrder.end()) {
        ExecOrder.emplace_back(op);
    }
}

int DAGManager::EdgesCmp(DAGEdgeInfo* a, DAGEdgeInfo* b) {
    return a->out_.estimated_cardinality > b->out_.estimated_cardinality;
}
}