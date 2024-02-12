#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/bloom_table_filter.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {
unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                                 optional_ptr<RelationStats> stats) {
    bool success = dag_manager.Build(*plan); 

    auto &sorted_nodes = dag_manager.getSortedOrder();
	// Forward
	for(auto i = 0; i < sorted_nodes.size() ; i++) {
        auto current_node = sorted_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return BF and its corresponding table id
		vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> BFvec = CreateBloomFilter(*current_node, false);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
			dag_manager.Add(BF.first, current_node->GetTableIndex()[0], BF.second);
		}
	}
	//Backward
	for(int i = sorted_nodes.size() - 1; i >= 0; i--) {
        auto &current_node = sorted_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return BF and its corresponding table id
		vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> BFvec = CreateBloomFilter(*current_node, true);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
		 	dag_manager.Add(current_node->GetTableIndex()[0], BF.first, BF.second);
		}
	}
	return plan;
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

vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node, bool reverse) {
    vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> result;
	idx_t cur = node.GetTableIndex()[0];
	duckdb::vector<duckdb::idx_t> col_mapping;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		col_mapping = get.column_ids;
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &get = node.children[0]->Cast<LogicalGet>();
		col_mapping = get.column_ids;
	}
	if(!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->in_) {
			for(auto bloom_filter : edge->bloom_filters) {
				if (node.type == LogicalOperatorType::LOGICAL_GET) {
					auto &get = node.Cast<LogicalGet>();
					get.table_filters.filters.clear();
					auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter.get());	
					get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
				} else if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
					auto &get = node.children[0]->Cast<LogicalGet>();
					get.table_filters.filters.clear();
					auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter.get());
					col_mapping = get.column_ids;
					get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
				}
			}
		}
	} else {
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			for(auto bloom_filter : edge->bloom_filters) {
				if (node.type == LogicalOperatorType::LOGICAL_GET) {
					auto &get = node.Cast<LogicalGet>();
					get.table_filters.filters.clear();
					auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter.get());
					col_mapping = get.column_ids;
					get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
				} else if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
					auto &get = node.children[0]->Cast<LogicalGet>();
					get.table_filters.filters.clear();
					auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter.get());
					col_mapping = get.column_ids;
					get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
				}
			}
		}
	}
	unique_ptr<LogicalOperator> node_copy;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		node_copy = get.FastCopy();
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &get = node.children[0]->Cast<LogicalGet>();
		node_copy = get.FastCopy();
	}
	// Create Bloom Filter
	unordered_map<idx_t, BlockedBloomFilter*> temp_result;
	// this table's col_id to next tables' columnbinding
	unordered_map<idx_t, vector<ColumnBinding>> this_to_next;
	if (!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			// Each Expression leads to a bloom filter on a column on this table
			for (auto &expr : edge->filters) {
				vector<BoundColumnRefExpression*> expressions;
				GetColumnBindingExpression(*expr, expressions);
				D_ASSERT(expressions.size() == 2);
				if (expressions[0]->binding.table_index == cur) {
					if (temp_result.find(expressions[0]->binding.column_index) == temp_result.end()) {
						auto cur_filter = new BlockedBloomFilter(expressions[0]->binding);
						// insert origin column id
						temp_result[expressions[0]->binding.column_index] = cur_filter;
						this_to_next[expressions[0]->binding.column_index].emplace_back(expressions[1]->binding);
					}
				} else if (expressions[1]->binding.table_index == cur) {
					// insert origin column id
					if (temp_result.find(expressions[1]->binding.column_index) == temp_result.end()) {
						auto cur_filter = new BlockedBloomFilter(expressions[1]->binding);
						// insert origin column id
						temp_result[expressions[1]->binding.column_index] = cur_filter;
						this_to_next[expressions[1]->binding.column_index].emplace_back(expressions[0]->binding);
					}
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
					if (temp_result.find(expressions[0]->binding.column_index) == temp_result.end()) {
						auto cur_filter = new BlockedBloomFilter(expressions[0]->binding);
						// insert origin column id
						temp_result[expressions[0]->binding.column_index] = cur_filter;
						this_to_next[expressions[0]->binding.column_index].emplace_back(expressions[1]->binding);
					}
				} else if (expressions[1]->binding.table_index == cur) {
					auto cur_filter = new BlockedBloomFilter(expressions[1]->binding);
					// insert origin column id
					if (temp_result.find(expressions[1]->binding.column_index) == temp_result.end()) {
						// insert origin column id
						temp_result[expressions[1]->binding.column_index] = cur_filter;
						this_to_next[expressions[1]->binding.column_index].emplace_back(expressions[0]->binding);
					}
				}
			}
		}
	}
	// Start Physical Plan
	PhysicalPlanGenerator physical_planner(this->context);
	auto physical_plan = physical_planner.CreatePlan(std::move(node_copy));
	auto executor = make_uniq<Executor>(this->context);
	executor->Initialize(std::move(physical_plan));
	auto flag = executor->ExecuteTask();
	auto data = executor->FetchChunk();
	int itr = 0;
	unordered_map<idx_t, shared_ptr<BloomFilterBuilder_SingleThreaded>> temp_builder;
	while (data->size() != 0) {
		if(itr == 0) {
			for (auto &filter : temp_result) {
				auto builder = make_shared<BloomFilterBuilder_SingleThreaded>();
				builder->Begin(1, 1, arrow::MemoryPool::CreateDefault().get(), node.estimated_cardinality, 1000, filter.second);
				temp_builder[filter.first] = builder;
			}
		}
		for(auto &filter : temp_builder) {
			auto type = data->data[filter.first].GetType();
			uint64_t* hashes = new uint64_t[data->size()];
			for(auto r = 0; r < data->size(); r++) {
				uint64_t hash = 0;
				hash = data->GetValue(filter.first, r).Hash();
				hashes[r] = hash;
			}
			filter.second->PushNextBatch(1, data->size(), hashes);
		}
		data = executor->FetchChunk();
		itr++;
	}
	
	// Create Bloom Table Filter at this table
	for(auto pair : temp_result) {
		if (node.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = node.Cast<LogicalGet>();
			auto bf = pair.second;
			auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bf);
			get.table_filters.PushFilter(col_mapping[bf->GetCol().column_index], std::move(bloom_table_filter));
		} else if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &get = node.children[0]->Cast<LogicalGet>();
			auto bf = pair.second;
			auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bf);
			get.table_filters.PushFilter(col_mapping[bf->GetCol().column_index], std::move(bloom_table_filter));
			// node = get;
		}
	}

	// Prepare bloom filter for next table
	for(auto &filter : temp_result) {
		auto bf = filter.second;
		for(auto column_binding : this_to_next[bf->GetCol().column_index]) {
			auto new_bf = bf->CopywithNewColBinding(column_binding);
			result.emplace_back(make_pair(new_bf->GetCol().table_index, new_bf));
		}
	}

	return result;
}
}