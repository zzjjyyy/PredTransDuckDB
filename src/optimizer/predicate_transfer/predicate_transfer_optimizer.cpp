#include "duckdb/optimizer/predicate_transfer/predicate_transfer_optimizer.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/bloom_table_filter.hpp"
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

namespace duckdb {
LogicalGet& PredicateTransferOptimizer::LogicalGetinFilter(LogicalOperator &op) {
	if (op.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		return op.children[0]->Cast<LogicalGet>();
	} else if (op.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		// For In-Clause optimization
		return op.children[0]->children[0]->Cast<LogicalGet>();
	}
}

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
		vector<pair<BlockedBloomFilter*, BlockedBloomFilter*>> BFvec = CreateBloomFilter(*current_node, false);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
			dag_manager.Add(BF.first, BF.second);
		}
	}
	//Backward
	/*
	for(int i = sorted_nodes.size() - 1; i >= 0; i--) {
        auto &current_node = sorted_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return BF and its corresponding table id
		vector<pair<BlockedBloomFilter*, BlockedBloomFilter*>> BFvec = CreateBloomFilter(*current_node, true);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
		 	dag_manager.Add(BF.second, BF.first);
		}
	}
	*/
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

/* Further to do, test filter operator */
vector<pair<BlockedBloomFilter*, BlockedBloomFilter*>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node, bool reverse) {
    vector<pair<BlockedBloomFilter*, BlockedBloomFilter*>> result;
	idx_t cur;
	duckdb::vector<duckdb::idx_t> col_mapping;
	// SchemaCatalogEntry *entry;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		col_mapping = get.column_ids;
		cur = node.GetTableIndex()[0];
		// auto table_scan_bind_data = get.bind_data->Cast<TableScanBindData>();
		// entry = &table_scan_bind_data.table.schema;
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &get = LogicalGetinFilter(node);
		col_mapping = get.column_ids;
		cur = get.GetTableIndex()[0];
		// auto table_scan_bind_data = get.bind_data->Cast<TableScanBindData>();
		// entry = &table_scan_bind_data.table.schema;
	}
	if(!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->in_) {
			for(auto bloom_filter : edge->bloom_filters) {
				if(!bloom_filter->isUsed())  {
					bloom_filter->setUsed();
					if (node.type == LogicalOperatorType::LOGICAL_GET) {
						auto &get = node.Cast<LogicalGet>();
						auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter);	
						get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
					} else if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
						auto &get = LogicalGetinFilter(node);
						auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter);
						get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
					}
				}
			}
		}
	} else {
		/* Further to do, remove the duplicate blooom filter */
		for(auto &edge : dag_manager.nodes.nodes[cur]->out_) {
			for(auto bloom_filter : edge->bloom_filters) {
				if(!bloom_filter->isUsed())  {
					bloom_filter->setUsed();
					if (node.type == LogicalOperatorType::LOGICAL_GET) {
						auto &get = node.Cast<LogicalGet>();
						auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter);
						get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
					} else if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
						auto &get = LogicalGetinFilter(node);
						auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bloom_filter);
						get.table_filters.PushFilter(col_mapping[bloom_filter->GetCol().column_index], std::move(bloom_table_filter));
					}
				}
			}
		}
	}
	
	unique_ptr<LogicalOperator> node_copy;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		node_copy = get.FastCopy();
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = node.Cast<LogicalFilter>();
		node_copy = filter.FastCopy();
	}

	auto name = "temp" + std::to_string(table_num++);;
	/*
	auto create_info = make_uniq<CreateInfo>(CatalogType::TABLE_ENTRY);
	create_info->catalog = name;
	create_info->internal = false;
	create_info->temporary = true;
	auto bound_create_info = make_uniq<BoundCreateTableInfo>(*entry, std::move(create_info));
	auto create_temp = make_uniq<LogicalCreateTable>(*entry, std::move(bound_create_info));
	create_temp->children.emplace_back(std::move(node_copy));
	PhysicalPlanGenerator create_physical_planner(this->context);
	auto create_physical_plan = create_physical_planner.CreatePlan(std::move(create_temp));
	auto create_executor = make_uniq<Executor>(this->context);
	create_executor->Initialize(std::move(create_physical_plan));
	create_executor->ExecuteTask();
	create_executor->CancelTasks();
	*/

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


	/*
	auto &get = LogicalGetinFilter(node);
	auto &original_data = get.bind_data->Cast<TableScanBindData>();
	auto table_entry = Catalog::GetEntry(this->context, CatalogType::TABLE_ENTRY, "", "", name, OnEntryNotFound::RETURN_NULL);
	original_data.table = table_entry->Cast<DuckTableEntry>();
	unique_ptr<LogicalOperator> new_node_copy;
	if (node.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = node.Cast<LogicalGet>();
		new_node_copy = get.FastCopy();
	} else if (node.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = node.Cast<LogicalFilter>();
		new_node_copy = filter.FastCopy();
	}
	*/

	// Start Physical Plan
	PhysicalPlanGenerator physical_planner(this->context);
	auto physical_plan = physical_planner.CreatePlan(std::move(node_copy));
	auto executor = make_uniq<Executor>(this->context);
	unique_ptr<PhysicalResultCollector> collector;
	auto &client_config = ClientConfig::GetConfig(this->context);
	auto get_method = client_config.result_collector ? client_config.result_collector : PhysicalResultCollector::GetResultCollector;
	shared_ptr<PreparedStatementData> statement = make_shared<PreparedStatementData>(StatementType::SELECT_STATEMENT);
	for(int i = 0; i < physical_plan->types.size(); ++i) {
		statement->names.emplace_back(std::to_string(i));
	}
	statement->types = physical_plan->types;
	statement->plan = std::move(physical_plan);
	collector = get_method(this->context, *statement);
	executor->Initialize(std::move(collector));
	try {
		PendingExecutionResult flag;
		do {
			flag = executor->ExecuteTask();
		} while(flag != PendingExecutionResult::EXECUTION_ERROR
			 && flag != PendingExecutionResult::RESULT_READY);
	} catch (FatalException &ex) {
		throw ex;
	} catch (const Exception &ex) {
		throw ex;
	} catch (std::exception &ex) {
		throw ex;
	} catch (...) { // LCOV_EXCL_START
		throw InternalException("Unhandled exception in CreateBloomFilter");
	} // LCOV_EXCL_STOP
	auto query_result = executor->GetResult();
	executor->CancelTasks();
	auto data = query_result->Fetch();
	/*
	executor->Initialize(std::move(physical_plan));
	auto flag = executor->ExecuteTask();
	auto data = executor->FetchChunk();
	*/
	int itr = 0;
	unordered_map<idx_t, shared_ptr<BloomFilterBuilder_SingleThreaded>> temp_builder;
	while (data != nullptr) {
		if(itr == 0) {
			for (auto &filter : temp_result) {
				auto builder = make_shared<BloomFilterBuilder_SingleThreaded>();
				builder->Begin(1, 1, arrow::MemoryPool::CreateDefault().get(), node.estimated_cardinality, 1000, filter.second);
				temp_builder[filter.first] = builder;
			}
		}
		for(auto &filter : temp_builder) {
			Vector hashes(LogicalType::HASH);
			VectorOperations::Hash(data->data[filter.first], hashes, data->size());
			filter.second->PushNextBatch(1, data->size(), (hash_t*)hashes.GetData());
		}
		// data = executor->FetchChunk();
		data = query_result->Fetch();
		itr++;
	}
	
	// Create Bloom Table Filter at this table
	for(auto pair : temp_result) {
		if(!pair.second->isUsed()) {
			pair.second->setUsed();
			/*
			if (node.type == LogicalOperatorType::LOGICAL_GET) {
				auto &get = node.Cast<LogicalGet>();
				auto bf = pair.second;
				auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bf);
			} else if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
				auto &get = node.children[0]->Cast<LogicalGet>();
				auto bf = pair.second;
				auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bf);
			}
			*/
			// The new-created bloom filter of LogicalGet is redundant, so we do not push it
			if(node.type == LogicalOperatorType::LOGICAL_FILTER) {
				auto &get = node.children[0]->Cast<LogicalGet>();
				auto bf = pair.second;
				auto bloom_table_filter = make_uniq<BloomTableFilter>(TableFilterType::BLOOM_FILTER, bf);
				get.table_filters.PushFilter(col_mapping[bf->GetCol().column_index], std::move(bloom_table_filter));
			}
		}
	}

	// Prepare bloom filter for next table
	for(auto &filter : temp_result) {
		auto bf = filter.second;
		for(auto column_binding : this_to_next[bf->GetCol().column_index]) {
			auto new_bf = bf->CopywithNewColBinding(column_binding);
			result.emplace_back(make_pair(bf, new_bf));
		}
	}

	return result;
}
}