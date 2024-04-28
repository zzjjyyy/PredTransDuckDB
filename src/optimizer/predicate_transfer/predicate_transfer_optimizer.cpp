#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include <set>

namespace duckdb {
LogicalGet& PredicateTransferOptimizer::LogicalGetinFilter(LogicalOperator &op) {
	if (op.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		return op.children[0]->Cast<LogicalGet>();
	} else if (op.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// For In-Clause optimization
		return op.children[0]->children[0]->Cast<LogicalGet>();
	}
}

/* For PredSingleTransfer */
/*
unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                                 optional_ptr<RelationStats> stats) {
    bool success = dag_manager.Build(*plan); 
	if(!success) {
		return plan;
	}
    auto &sorted_nodes = dag_manager.getSortedOrder();
	// Forward
	for(auto i = 0; i < sorted_nodes.size(); i++) {
        auto current_node = sorted_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return current BF and neighbor BF
		auto BFvec = CreateBloomFilter(*current_node, false);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
			dag_manager.Add(BF.first, BF.second, false);
		}
	}
	auto result = InsertCreateBFOperator(std::move(plan));
	return result;
}
*/

/* For PredTransfer */
unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                                 optional_ptr<RelationStats> stats) {
	bool success = dag_manager.Build(*plan); 
	if(!success) {
		return plan;
	}
    auto &sorted_nodes = dag_manager.getSortedOrder();
	// Forward
	for(auto i = 0; i < sorted_nodes.size(); i++) {
        auto current_node = sorted_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return current BF and neighbor BF
		auto BFvec = CreateBloomFilter(*current_node, false);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
			dag_manager.Add(BF.first, BF.second, false);
		}
	}	
	
	//Backward
	for(int i = sorted_nodes.size() - 1; i >= 0; i--) {
        auto &current_node = sorted_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return BF and its corresponding table id
		auto BFvec = CreateBloomFilter(*current_node, true);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
		 	dag_manager.Add(BF.first, BF.second, true);
		}
	}
	auto result = InsertCreateBFOperator_d(std::move(plan));
	return result;
}

void PredicateTransferOptimizer::GetColumnBindingExpression(Expression &expr, vector<BoundColumnRefExpression*> &expressions) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		Expression* expr_ptr = &expr;
		BoundColumnRefExpression* colref = static_cast<BoundColumnRefExpression*>(expr_ptr);
		D_ASSERT(colref->depth == 0);
		expressions.emplace_back(colref);
	} else {
		ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) { GetColumnBindingExpression(*child, expressions); });
	}
}

/* Further to do, test filter operator */
vector<pair<ColumnBinding, shared_ptr<BlockedBloomFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node, bool reverse) {
	vector<pair<ColumnBinding, shared_ptr<BlockedBloomFilter>>> result;
	idx_t cur = GetNodeId(node);
	if(dag_manager.nodes.nodes.find(cur) == dag_manager.nodes.nodes.end()) {
		return result;
	}
	// Use Bloom Filter
	vector<shared_ptr<BlockedBloomFilter>> temp_result_to_use;
	vector<idx_t> depend_nodes;
	GetAllBFUsed(cur, temp_result_to_use, depend_nodes, reverse);
	
	// Create Bloom Filter
	unordered_map<idx_t, vector<shared_ptr<BlockedBloomFilter>>> temp_result_to_create;
	GetAllBFCreate(cur, temp_result_to_create, reverse);
	
	if(temp_result_to_use.size() == 0) {
		if(temp_result_to_create.size() == 0) {
			return result;
		} else {
			auto create_bf = BuildSingleCreateOperator(node, temp_result_to_create);
			for (auto &filter_vec : create_bf->bf_to_create) {
				for(auto bf : filter_vec.second) {
					result.emplace_back(make_pair(ColumnBinding(cur, filter_vec.first), bf));
				}
			}
			if(!reverse) {
				replace_map_forward[&node] = std::move(create_bf);
			} else {
				replace_map_backward[&node] = std::move(create_bf);
			}
			return result;
		}
	} else {
		if (temp_result_to_create.size() == 0) {
			auto use_bf = BuildUseOperator(node, temp_result_to_use, depend_nodes, reverse);
			if(!reverse) {
				replace_map_forward[&node] = std::move(use_bf);
			} else {
				replace_map_backward[&node] = std::move(use_bf);
			}
			return result;
		} else {
			auto create_bf = BuildCreateUsePair(node, temp_result_to_use, temp_result_to_create, depend_nodes, reverse);
			for (auto &filter_vec : create_bf->bf_to_create) {
				for(auto bf : filter_vec.second) {
					result.emplace_back(make_pair(ColumnBinding(cur, filter_vec.first), bf));
				}
			}
			if(!reverse) {
				replace_map_forward[&node] = std::move(create_bf);
			} else {
				replace_map_backward[&node] = std::move(create_bf);
			}
			return result;
		}
	}
}

idx_t PredicateTransferOptimizer::GetNodeId(LogicalOperator &node) {
	idx_t res = -1;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		res = get.GetTableIndex()[0];
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &get = LogicalGetinFilter(node);
		res = get.GetTableIndex()[0];
	} else if (node.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		auto &delim_get = node.Cast<LogicalDelimGet>();
		res = delim_get.GetTableIndex()[0];
	}
	return res;
}

void PredicateTransferOptimizer::GetAllBFUsed(idx_t cur, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use, vector<idx_t> &depend_nodes, bool reverse) {
	if(!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->in_) {
			for(auto bloom_filter : edge->bloom_filters) {
				if(!bloom_filter->isUsed())  {
					bloom_filter->setUsed();
					temp_result_to_use.emplace_back(bloom_filter);
					depend_nodes.emplace_back(edge->dest_);
				}
			}
		}
	} else {
		/* Further to do, remove the duplicate blooom filter */
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			for(auto bloom_filter : edge->bloom_filters) {
				if(!bloom_filter->isUsed())  {
					bloom_filter->setUsed();
					temp_result_to_use.emplace_back(bloom_filter);
					depend_nodes.emplace_back(edge->dest_);
				}
			}
		}
	}
}

void PredicateTransferOptimizer::GetAllBFCreate(idx_t cur, unordered_map<idx_t, vector<shared_ptr<BlockedBloomFilter>>> &temp_result_to_create, bool reverse) {
	if (!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			// Each Expression leads to a bloom filter on a column on this table
			for (auto &expr : edge->filters) {
				vector<BoundColumnRefExpression*> expressions;
				GetColumnBindingExpression(*expr, expressions);
				D_ASSERT(expressions.size() == 2);
				if (expressions[0]->binding.table_index == cur) {
					auto cur_filter = make_shared<BlockedBloomFilter>(expressions[1]->binding);
					// insert origin column id
					temp_result_to_create[expressions[0]->binding.column_index].emplace_back(cur_filter);
				} else if (expressions[1]->binding.table_index == cur) {
					// insert origin column id
					auto cur_filter = make_shared<BlockedBloomFilter>(expressions[0]->binding);
					// insert origin column id
					temp_result_to_create[expressions[1]->binding.column_index].emplace_back(cur_filter);
				}
			}
		}
	} else {
		for(auto &edge : dag_manager.nodes.nodes[cur]->in_) {
			// Each Expression leads to a bloom filter on a column on this table
			for (auto &expr : edge->filters) {
				vector<BoundColumnRefExpression*> expressions;
				GetColumnBindingExpression(*expr, expressions);
				D_ASSERT(expressions.size() == 2);
				if (expressions[0]->binding.table_index == cur) {
					auto cur_filter = make_shared<BlockedBloomFilter>(expressions[1]->binding);
					// insert origin column id
					temp_result_to_create[expressions[0]->binding.column_index].emplace_back(cur_filter);
				} else if (expressions[1]->binding.table_index == cur) {
					// insert origin column id
					auto cur_filter = make_shared<BlockedBloomFilter>(expressions[0]->binding);
					// insert origin column id
					temp_result_to_create[expressions[1]->binding.column_index].emplace_back(cur_filter);
				}
			}
		}
	}
}

unique_ptr<LogicalCreateBF>
PredicateTransferOptimizer::BuildSingleCreateOperator(LogicalOperator &node,
											 		  unordered_map<idx_t, vector<shared_ptr<BlockedBloomFilter>>> &temp_result_to_create) {
	auto create_bf = make_uniq<LogicalCreateBF>(temp_result_to_create);
	create_bf->has_estimated_cardinality = true;
	create_bf->estimated_cardinality = node.estimated_cardinality;
	return create_bf;
}

unique_ptr<LogicalUseBF>
PredicateTransferOptimizer::BuildUseOperator(LogicalOperator &node,
											 vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use,
											 vector<idx_t> &depend_nodes,
											 bool reverse) {
	D_ASSERT(temp_result_to_use.size() == depend_nodes.size());
	unique_ptr<LogicalUseBF> pre_use_bf;
	unique_ptr<LogicalUseBF> use_bf;
	// This is important for performance, not use for (int i = 0; i < temp_result_to_use.size(); i++)
	for (int i = temp_result_to_use.size() - 1; i >= 0; i--) {
		vector<shared_ptr<BlockedBloomFilter>> v;
		v.emplace_back(temp_result_to_use[i]);
		use_bf = make_uniq<LogicalUseBF>(v);
		use_bf->has_estimated_cardinality = true;
		use_bf->estimated_cardinality = node.estimated_cardinality;
		auto idx = depend_nodes[i];
		auto base_node = dag_manager.nodes_manager.getNode(idx);
		if (!reverse) {
			auto related_bf_create = replace_map_forward[base_node].get();
			if(related_bf_create->type == LogicalOperatorType::LOGICAL_CREATE_BF) {
				use_bf->AddDownStreamOperator((LogicalCreateBF*)related_bf_create);
			} else {
				D_ASSERT(false);
			}
		} else {
			auto related_bf_create = replace_map_backward[base_node].get();
			if(related_bf_create->type == LogicalOperatorType::LOGICAL_CREATE_BF) {
				use_bf->AddDownStreamOperator((LogicalCreateBF*)related_bf_create);
			} else {
				D_ASSERT(false);
			}
		}
		if (pre_use_bf != nullptr) {
			use_bf->AddChild(std::move(pre_use_bf));
		}
		pre_use_bf = std::move(use_bf);
	}
	return pre_use_bf;
}

unique_ptr<LogicalCreateBF>
PredicateTransferOptimizer::BuildCreateUsePair(LogicalOperator &node,
											   vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use,
											   unordered_map<idx_t, vector<shared_ptr<BlockedBloomFilter>>> &temp_result_to_create,
											   vector<idx_t> &depend_nodes,
											   bool reverse) {
	auto use_bf = BuildUseOperator(node, temp_result_to_use, depend_nodes, reverse);
	auto create_bf = make_uniq<LogicalCreateBF>(temp_result_to_create);
	create_bf->AddChild(unique_ptr_cast<LogicalUseBF, LogicalOperator>(std::move(use_bf)));
	create_bf->has_estimated_cardinality = true;
	create_bf->estimated_cardinality = node.estimated_cardinality;
	return create_bf;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertCreateBFOperator(unique_ptr<LogicalOperator> plan) {
	for(auto &child : plan->children) {
		child = InsertCreateBFOperator(std::move(child));
	}
	void *plan_ptr = plan.get();
	auto itr = replace_map_forward.find(plan_ptr);
	if (itr != replace_map_forward.end()) {
		auto ptr = itr->second.get();
		while (ptr->children.size() != 0) {
			ptr = ptr->children[0].get();
		}
		ptr->AddChild(std::move(plan));
		return std::move(itr->second);
	} else {
		return plan;
	}
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertCreateBFOperator_d(unique_ptr<LogicalOperator> plan) {
	for(auto &child : plan->children) {
		child = InsertCreateBFOperator_d(std::move(child));
	}
	void *plan_ptr = plan.get();
	auto itr = replace_map_forward.find(plan_ptr);
	if (itr != replace_map_forward.end()) {
		auto ptr = itr->second.get();
		while (ptr->children.size() != 0) {
			ptr = ptr->children[0].get();
		}
		ptr->AddChild(std::move(plan));
		plan = std::move(itr->second);
		auto itr_next = replace_map_backward.find(plan_ptr);
		if (itr_next != replace_map_backward.end()) {
			auto ptr_next = itr_next->second.get();
			while (ptr_next->children.size() != 0) {
				ptr_next = ptr_next->children[0].get();
			}
			ptr_next->AddChild(std::move(plan));
			return std::move(itr_next->second);
		} else {
			return plan;
		}
	} else {
		return plan;
	}
}
}