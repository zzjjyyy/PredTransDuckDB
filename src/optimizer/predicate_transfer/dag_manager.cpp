#include "duckdb/optimizer/predicate_transfer/dag_manager.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include <queue>

namespace duckdb {
bool DAGManager::Build(LogicalOperator &plan) {
    vector<reference<LogicalOperator>> filter_operators;
    nodes_manager.ExtractNodes(plan, filter_operators);
    auto& nodes = nodes_manager.getNodes();
    if(nodes_manager.NumNodes() < 2) {
        return false;
    }
    nodes_manager.SortNodes();
    // extract the edges of the hypergraph, creating a list of filters and their associated bindings.
	ExtractEdges(plan, filter_operators);
    if(filters_and_bindings_.size() == 0) {
        return false;
    }
    for (auto itr = nodes.begin(); itr != nodes.end();) {
        auto v = GetNeighbors(itr->first);
        if (v.size() == 0) {
            auto &sorted = nodes_manager.getSortedNodes();
            sorted.erase(std::find(sorted.begin(), sorted.end(), itr->second));
            itr = nodes_manager.getNodes().erase(itr);
        } else {
            itr++;
        }
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

struct DAGNodeCompare {
    bool operator()(const DAGNode *lhs, const DAGNode *rhs) const {
        return lhs->size < rhs->size;
    }
};

void DAGManager::LargestFirst(vector<LogicalOperator*> &sorted_nodes) {
    std::priority_queue<DAGNode*, vector<DAGNode*>, DAGNodeCompare> list;
    unordered_set<idx_t> met;
    // Create Vertices
    for(auto &vertex : nodes_manager.getNodes()) {
        // Set the last operator as root
        if(vertex.second == sorted_nodes.back()) {
            auto node = make_uniq<DAGNode>(vertex.first, vertex.second->estimated_cardinality, true);
            list.push(node.get());
            met.emplace(node->Id());
            nodes.nodes[vertex.first] = std::move(node);
        } else {
            nodes.nodes[vertex.first] = make_uniq<DAGNode>(vertex.first, vertex.second->estimated_cardinality, false);
        }
    }
    int prior_flag = nodes_manager.NumNodes() - 1;
    while(!list.empty()) {
        auto node = list.top();
        list.pop();
        node->priority = prior_flag--;
        auto neighbors = GetNeighbors(node->Id());
        for(auto i : neighbors) {
            if (met.find(i->Id()) == met.end()) {
                list.push(i);
                met.emplace(i->Id());
            }
        }
        ExecOrder.emplace_back(nodes_manager.getNode(node->Id()));
    }
}

void DAGManager::RandomFirst(vector<LogicalOperator*> &sorted_nodes) {
    std::priority_queue<DAGNode*, vector<DAGNode*>, DAGNodeCompare> list;
    unordered_set<idx_t> met;
    std::uniform_int_distribution<int> dist_1(0, nodes_manager.NumNodes() - 1);
    std::uniform_int_distribution<idx_t> dist_2(0, 99999999999);
	int root_id = dist_1(g);
    // Create Vertices
    for(auto &vertex : nodes_manager.getNodes()) {
        // Set the last operator as root
        if (vertex.second == sorted_nodes[root_id]) {
            auto node = make_uniq<DAGNode>(vertex.first, dist_2(g), true);
            list.push(node.get());
            met.emplace(node->Id());
            nodes.nodes[vertex.first] = std::move(node);
        } else {
            nodes.nodes[vertex.first] = make_uniq<DAGNode>(vertex.first, dist_2(g), false);
        }
    }
    int prior_flag = nodes_manager.NumNodes() - 1;
    while(!list.empty()) {
        auto node = list.top();
        list.pop();
        node->priority = prior_flag--;
        auto neighbors = GetNeighbors(node->Id());
        for(auto i : neighbors) {
            if (met.find(i->Id()) == met.end()) {
                list.push(i);
                met.emplace(i->Id());
            }
        }
        ExecOrder.emplace_back(nodes_manager.getNode(node->Id()));
    }
}

void DAGManager::CreateDAG() {
    auto &sorted_nodes = nodes_manager.getSortedNodes();
    LargestFirst(sorted_nodes);
    // RandomFirst(sorted_nodes);
    for (auto &filter_and_binding : filters_and_bindings_) {
        if(filter_and_binding) {
            idx_t large;
            if (filter_and_binding->large_.type == LogicalOperatorType::LOGICAL_GET) {
                large = filter_and_binding->large_.GetTableIndex()[0];
            } else if (filter_and_binding->large_.type == LogicalOperatorType::LOGICAL_FILTER) {
                LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(filter_and_binding->large_);
                large = get.GetTableIndex()[0];
            } else if (filter_and_binding->large_.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                large = filter_and_binding->large_.GetTableIndex()[0];
            } else if (filter_and_binding->large_.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                large = filter_and_binding->large_.GetTableIndex()[0];
            }
            idx_t small;
            if (filter_and_binding->small_.type == LogicalOperatorType::LOGICAL_GET) {
                small = filter_and_binding->small_.GetTableIndex()[0];
            } else if (filter_and_binding->small_.type == LogicalOperatorType::LOGICAL_FILTER) {
                LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(filter_and_binding->small_);
                small = get.GetTableIndex()[0];
            } else if (filter_and_binding->small_.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                small = filter_and_binding->small_.GetTableIndex()[0];
            } else if (filter_and_binding->small_.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                small = filter_and_binding->small_.GetTableIndex()[0];
            }
            auto small_node = nodes.nodes[small].get();
            auto large_node = nodes.nodes[large].get();
            // smaller one has higher priority
            if(small_node->priority > large_node->priority) {
                small_node->AddIn(large_node->Id(), filter_and_binding->filter.get());
                large_node->AddOut(small_node->Id(), filter_and_binding->filter.get());
            } else {
                small_node->AddOut(large_node->Id(), filter_and_binding->filter.get());
                large_node->AddIn(small_node->Id(), filter_and_binding->filter.get());
            }
        }
    }
}

vector<DAGNode*> DAGManager::GetNeighbors(idx_t node_id) {
    vector<DAGNode*> result;
    for (auto &filter_and_binding : filters_and_bindings_) {
        if(filter_and_binding) {
            if (&filter_and_binding->large_ == nodes_manager.getNode(node_id)) {
                auto &op = filter_and_binding->small_;
                idx_t another_node_id;
                if (op.type == LogicalOperatorType::LOGICAL_GET) {
                    another_node_id = op.GetTableIndex()[0];
                } else if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
                    LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(op);
                    another_node_id = get.GetTableIndex()[0];
                } else if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                    another_node_id = op.GetTableIndex()[0];
                } else if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                    another_node_id = op.GetTableIndex()[0];
                }
                result.emplace_back(nodes.nodes[another_node_id].get());
            } else if(&filter_and_binding->small_ == nodes_manager.getNode(node_id)) {
                auto &op = filter_and_binding->large_;
                idx_t another_node_id;
                if (op.type == LogicalOperatorType::LOGICAL_GET) {
                    another_node_id = op.GetTableIndex()[0];
                } else if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
                    LogicalGet &get = PredicateTransferOptimizer::LogicalGetinFilter(op);
                    another_node_id = get.GetTableIndex()[0];
                } else if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
                    another_node_id = op.GetTableIndex()[0];
                } else if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
                    another_node_id = op.GetTableIndex()[0];
                }
                result.emplace_back(nodes.nodes[another_node_id].get());
            }
        }
    }
    return result;
}

int DAGManager::DAGNodesCmp(DAGNode* a, DAGNode* b) {
    return a->size > b->size;
}
}