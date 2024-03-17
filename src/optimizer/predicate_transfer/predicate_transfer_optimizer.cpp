#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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
		vector<pair<ColumnBinding, BlockedBloomFilter*>> BFvec = CreateBloomFilter(*current_node, false);
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
		vector<pair<ColumnBinding, BlockedBloomFilter*>> BFvec = CreateBloomFilter(*current_node, false);
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
		vector<pair<ColumnBinding, BlockedBloomFilter*>> BFvec = CreateBloomFilter(*current_node, true);
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
vector<pair<ColumnBinding, BlockedBloomFilter*>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node, bool reverse) {
	vector<pair<ColumnBinding, BlockedBloomFilter*>> result;
	idx_t cur;
	vector<ColumnBinding> cur_col_binding = node.GetColumnBindings();
	vector<LogicalType> cur_types;
	duckdb::vector<duckdb::idx_t> col_mapping;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		col_mapping = get.column_ids;
		cur = get.GetTableIndex()[0];
		cur_types = get.returned_types;
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &get = LogicalGetinFilter(node);
		col_mapping = get.column_ids;
		cur = get.GetTableIndex()[0];
		cur_types = get.returned_types;
	}

	// Use Bloom Filter
	vector<BlockedBloomFilter*> temp_result_to_use;
	vector<idx_t> depend_nodes;
	if(!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->in_) {
			depend_nodes.emplace_back(edge->dest_);
			for(auto bloom_filter : edge->bloom_filters) {
				if(!bloom_filter->isUsed())  {
					bloom_filter->setUsed();
					temp_result_to_use.emplace_back(bloom_filter);
				}
			}
		}
	} else {
		/* Further to do, remove the duplicate blooom filter */
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			depend_nodes.emplace_back(edge->dest_);
			for(auto bloom_filter : edge->bloom_filters) {
				if(!bloom_filter->isUsed())  {
					bloom_filter->setUsed();
					temp_result_to_use.emplace_back(bloom_filter);
				}
			}
		}
	}
	
	auto use_bf = make_uniq<LogicalUseBF>(temp_result_to_use);
	use_bf->has_estimated_cardinality = true;
	use_bf->estimated_cardinality = node.estimated_cardinality;

	// Create Bloom Filter
	unordered_map<idx_t, vector<BlockedBloomFilter*>> temp_result_to_create;
	if (!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			// Each Expression leads to a bloom filter on a column on this table
			for (auto &expr : edge->filters) {
				vector<BoundColumnRefExpression*> expressions;
				GetColumnBindingExpression(*expr, expressions);
				D_ASSERT(expressions.size() == 2);
				if (expressions[0]->binding.table_index == cur) {
					auto cur_filter = new BlockedBloomFilter(expressions[1]->binding);
					// insert origin column id
					temp_result_to_create[expressions[0]->binding.column_index].emplace_back(cur_filter);
				} else if (expressions[1]->binding.table_index == cur) {
					// insert origin column id
					auto cur_filter = new BlockedBloomFilter(expressions[0]->binding);
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
					auto cur_filter = new BlockedBloomFilter(expressions[1]->binding);
					// insert origin column id
					temp_result_to_create[expressions[0]->binding.column_index].emplace_back(cur_filter);
				} else if (expressions[1]->binding.table_index == cur) {
					// insert origin column id
					auto cur_filter = new BlockedBloomFilter(expressions[0]->binding);
					// insert origin column id
					temp_result_to_create[expressions[1]->binding.column_index].emplace_back(cur_filter);
				}
			}
		}
	}

	/* Create LogicalCreateBF */
	auto create_bf = make_uniq<LogicalCreateBF>(temp_result_to_create);

	/* Add their children */
	create_bf->AddChild(unique_ptr_cast<LogicalUseBF, LogicalOperator>(std::move(use_bf)));
	create_bf->has_estimated_cardinality = true;
	create_bf->estimated_cardinality = node.estimated_cardinality;

	for(auto idx : depend_nodes) {
		auto base_node = dag_manager.nodes_manager.getNode(idx);
		if (!reverse) {
			auto related_bf_create = replace_map_forward[base_node].get();
			create_bf->AddDownStreamCreateBF(related_bf_create);
		} else {
			auto related_bf_create = replace_map_backward[base_node].get();
			create_bf->AddDownStreamCreateBF(related_bf_create);
		}
	}

	if(!reverse) {
		replace_map_forward[&node] = std::move(create_bf);
	} else {
		replace_map_backward[&node] = std::move(create_bf);
	}
	
	// Prepare bloom filter for next table
	for(auto &filter_vec : temp_result_to_create) {
		for(auto bf : filter_vec.second) {
			result.emplace_back(make_pair(ColumnBinding(cur, filter_vec.first), bf));
		}
	}

	return result;
}

unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertCreateBFOperator(unique_ptr<LogicalOperator> plan) {
	for(auto &child : plan->children) {
		child = InsertCreateBFOperator(std::move(child));
	}
	void *plan_ptr = plan.get();
	auto itr = replace_map_forward.find(plan_ptr);
	if (itr != replace_map_forward.end()) {
		itr->second->children[0]->AddChild(std::move(plan));
		return unique_ptr_cast<LogicalCreateBF, LogicalOperator>(std::move(itr->second));
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
		itr->second->children[0]->AddChild(std::move(plan));
		plan = std::move(itr->second);
		auto itr_next = replace_map_backward.find(plan_ptr);
		if (itr_next != replace_map_backward.end()) {
			itr_next->second->children[0]->AddChild(std::move(plan));
			return unique_ptr_cast<LogicalCreateBF, LogicalOperator>(std::move(itr_next->second));
		} else {
			return plan;
		}
	} else {
		return plan;
	}
}
}