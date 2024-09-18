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
#include "duckdb/planner/operator/logical_create_table_for_BF.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include <set>

namespace duckdb {
std::unordered_map<std::string, int> PredicateTransferOptimizer::table_exists;

LogicalGet& PredicateTransferOptimizer::LogicalGetinFilter(LogicalOperator &op) {
	if(op.type == LogicalOperatorType::LOGICAL_FILTER) {
		LogicalOperator* child = op.children[0].get();
		while (child->type != LogicalOperatorType::LOGICAL_GET) {
			child = child->children[0].get();
		}
		return child->Cast<LogicalGet>();
	} else if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalGet>();
	}
}

/* For PredTransfer */
/* Here is the entry of PredicateTransferOptimizer */
/* Given a query plan, we insert CreateBF and UseBF into it */
unique_ptr<LogicalOperator> PredicateTransferOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                                 optional_ptr<RelationStats> stats) {
	/* Build the DAG to decide the transfer order */
	bool success = dag_manager.Build(*plan); 
	if(!success) {
		return plan;
	}
    auto &ordered_nodes = dag_manager.getExecOrder();
	// Forward
	for(int i = ordered_nodes.size() - 1; i >= 0; i--) {
        auto current_node = ordered_nodes[i];
		// We do predicate transfer in the function CreateBloomFilter
		// query_graph_manager holds the input bloom filter
		// return BF and its corresponding table id
		auto BFvec = CreateBloomFilter(*current_node, false);
		for (auto &BF : BFvec) {
			// Add the Bloom Filter to its corresponding edge
			// Need to check whether the Bloom Filter needs to transfer
			// Such as, the column not involved in the predicate
			dag_manager.Add(BF.first, BF.second, false);
		}
	}	
	
	//Backward
	for(int i = 0; i < ordered_nodes.size(); i++) {
        auto &current_node = ordered_nodes[i];
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

/* which column(s) involved in this expression */
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

/* Create Bloom filter and use existing Bloom filter for the given scan or filter node */
/* Hash Filter or Bloom Filter */
vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node, bool reverse) {
// vector<pair<idx_t, shared_ptr<HashFilter>>> PredicateTransferOptimizer::CreateBloomFilter(LogicalOperator &node, bool reverse) {
	/* Hash Filter or Bloom Filter */
	// vector<pair<idx_t, shared_ptr<HashFilter>>> result;
	vector<pair<idx_t, shared_ptr<BlockedBloomFilter>>> result;
	idx_t cur = GetNodeId(node);
	if(dag_manager.nodes.nodes.find(cur) == dag_manager.nodes.nodes.end()) {
		return result;
	}
	// Use Bloom Filter
	/* Hash Filter or Bloom Filter */
	// vector<shared_ptr<HashFilter>> temp_result_to_use;
	vector<shared_ptr<BlockedBloomFilter>> temp_result_to_use;
	vector<idx_t> depend_nodes;
	GetAllBFUsed(cur, temp_result_to_use, depend_nodes, reverse);
	
	// Create Bloom Filter
	/* Hash Filter or Bloom Filter */
	// vector<shared_ptr<HashFilter>> temp_result_to_create;
	vector<shared_ptr<BlockedBloomFilter>> temp_result_to_create;
	GetAllBFCreate(cur, temp_result_to_create, reverse);
	
	if(temp_result_to_use.size() == 0) {
		if(temp_result_to_create.size() == 0) {
			return result;
		} else {
			if(!PossibleFilterAny(node, reverse)) {
				return result;
			}
			auto create_bf = BuildSingleCreateOperator(node, temp_result_to_create);
			for (auto &filter : create_bf->bf_to_create) {
				result.emplace_back(make_pair(cur, filter));
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
			for (auto &filter : create_bf->bf_to_create) {
				result.emplace_back(make_pair(cur, filter));
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
	switch(node.type) {
		case LogicalOperatorType::LOGICAL_GET:
		case LogicalOperatorType::LOGICAL_DELIM_GET:
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT: {
			res = node.GetTableIndex()[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto &get = LogicalGetinFilter(node);
			res = get.GetTableIndex()[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			res = node.GetTableIndex()[1];
			break;
		}
		default: {
            break;
        }
	}
	return res;
}

/* Hash Filter or Bloom Filter */
// void PredicateTransferOptimizer::GetAllBFUsed(idx_t cur, vector<shared_ptr<HashFilter>> &temp_result_to_use, vector<idx_t> &depend_nodes, bool reverse) {
void PredicateTransferOptimizer::GetAllBFUsed(idx_t cur, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use, vector<idx_t> &depend_nodes, bool reverse) {
	if(!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->forward_in_) {
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
		for(auto &edge : dag_manager.nodes.nodes[cur]->backward_in_) {
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

/* Hash Filter or Bloom Filter */
void PredicateTransferOptimizer::GetAllBFCreate(idx_t cur, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_create, bool reverse) {
// void PredicateTransferOptimizer::GetAllBFCreate(idx_t cur, vector<shared_ptr<HashFilter>> &temp_result_to_create, bool reverse) {
	if (!reverse) {
		for(auto &edge : dag_manager.nodes.nodes[cur]->forward_out_) {
			/* Hash Filter or Bloom Filter */
			// auto cur_filter = make_shared<HashFilter>();
			auto cur_filter = make_shared<BlockedBloomFilter>();
			// Each Expression leads to a bloom filter on a column on this table
			for (auto &expr : edge->filters) {
				vector<BoundColumnRefExpression*> expressions;
				GetColumnBindingExpression(*expr, expressions);
				if (dag_manager.nodes_manager.FindRename(expressions[0]->binding).table_index == cur) {
					cur_filter->AddColumnBindingApplied(dag_manager.nodes_manager.FindRename(expressions[1]->binding));
					cur_filter->AddColumnBindingBuilt(dag_manager.nodes_manager.FindRename(expressions[0]->binding));
				} else if (dag_manager.nodes_manager.FindRename(expressions[1]->binding).table_index == cur) {
					cur_filter->AddColumnBindingApplied(dag_manager.nodes_manager.FindRename(expressions[0]->binding));
					cur_filter->AddColumnBindingBuilt(dag_manager.nodes_manager.FindRename(expressions[1]->binding));
				}
			}
			temp_result_to_create.emplace_back(cur_filter);
		}
	} else {
		for(auto &edge : dag_manager.nodes.nodes[cur]->backward_out_) {
			/* Hash Filter or Bloom Filter */
			// auto cur_filter = make_shared<HashFilter>();
			auto cur_filter = make_shared<BlockedBloomFilter>();
			// Each Expression leads to a bloom filter on a column on this table
			for (auto &expr : edge->filters) {
				vector<BoundColumnRefExpression*> expressions;
				GetColumnBindingExpression(*expr, expressions);
				D_ASSERT(expressions.size() == 2);
				if (dag_manager.nodes_manager.FindRename(expressions[0]->binding).table_index == cur) {
					cur_filter->AddColumnBindingApplied(dag_manager.nodes_manager.FindRename(expressions[1]->binding));
					cur_filter->AddColumnBindingBuilt(dag_manager.nodes_manager.FindRename(expressions[0]->binding));
				} else if (dag_manager.nodes_manager.FindRename(expressions[1]->binding).table_index == cur) {
					cur_filter->AddColumnBindingApplied(dag_manager.nodes_manager.FindRename(expressions[0]->binding));
					cur_filter->AddColumnBindingBuilt(dag_manager.nodes_manager.FindRename(expressions[1]->binding));
				}
			}
			temp_result_to_create.emplace_back(cur_filter);
		}
	}
}

/* Hash Filter or Bloom Filter */
// unique_ptr<LogicalCreateBF> PredicateTransferOptimizer::BuildSingleCreateOperator(LogicalOperator &node, vector<shared_ptr<HashFilter>> &temp_result_to_create) {
unique_ptr<LogicalCreateBF> PredicateTransferOptimizer::BuildSingleCreateOperator(LogicalOperator &node, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_create) {
	auto create_bf = make_uniq<LogicalCreateBF>(temp_result_to_create);
	create_bf->has_estimated_cardinality = true;
	create_bf->estimated_cardinality = node.estimated_cardinality;
	return create_bf;
}

/* Hash Filter or Bloom Filter */
// unique_ptr<LogicalUseBF> PredicateTransferOptimizer::BuildUseOperator(LogicalOperator &node, vector<shared_ptr<HashFilter>> &temp_result_to_use, vector<idx_t> &depend_nodes, bool reverse)
unique_ptr<LogicalUseBF> PredicateTransferOptimizer::BuildUseOperator(LogicalOperator &node, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use, vector<idx_t> &depend_nodes, bool reverse) {
	D_ASSERT(temp_result_to_use.size() == depend_nodes.size());
	unique_ptr<LogicalUseBF> pre_use_bf;
	unique_ptr<LogicalUseBF> use_bf;
	// This is important for performance, not use (int i = 0; i < temp_result_to_use.size(); i++)
	for (int i = temp_result_to_use.size() - 1; i >= 0; i--) {
		/* Hash Filter or Bloom Filter */
		vector<shared_ptr<BlockedBloomFilter>> v;
		// vector<shared_ptr<HashFilter>> v;
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

/* Hash Filter or Bloom Filter */
// unique_ptr<LogicalCreateBF> PredicateTransferOptimizer::BuildCreateUsePair(LogicalOperator &node, vector<shared_ptr<HashFilter>> &temp_result_to_use, vector<shared_ptr<HashFilter>> &temp_result_to_create, vector<idx_t> &depend_nodes, bool reverse) {
unique_ptr<LogicalCreateBF> PredicateTransferOptimizer::BuildCreateUsePair(LogicalOperator &node, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_use, vector<shared_ptr<BlockedBloomFilter>> &temp_result_to_create, vector<idx_t> &depend_nodes, bool reverse) {
	auto use_bf = BuildUseOperator(node, temp_result_to_use, depend_nodes, reverse);
	auto create_bf = make_uniq<LogicalCreateBF>(temp_result_to_create);
	create_bf->AddChild(unique_ptr_cast<LogicalUseBF, LogicalOperator>(std::move(use_bf)));
	create_bf->has_estimated_cardinality = true;
	create_bf->estimated_cardinality = node.estimated_cardinality;
	return create_bf;
}

/* Insert CreateBF into the plan */
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

/* Only for microbenchmark */
unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertCreateBFOperator_d(unique_ptr<LogicalOperator> plan) {
	for(auto &child : plan->children) {
		child = InsertCreateBFOperator_d(std::move(child));
	}
	void *plan_ptr = plan.get();
	auto itr = replace_map_forward.find(plan_ptr);
	bool insert_create_table = false;
	if (itr != replace_map_forward.end()) {
		insert_create_table = true;
		auto ptr = itr->second.get();
		while (ptr->children.size() != 0) {
			ptr = ptr->children[0].get();
		}
		ptr->AddChild(std::move(plan));
		plan = std::move(itr->second);
	}
	auto itr_next = replace_map_backward.find(plan_ptr);
	if (itr_next != replace_map_backward.end()) {
		insert_create_table = true;
		auto ptr_next = itr_next->second.get();
		while (ptr_next->children.size() != 0) {
			ptr_next = ptr_next->children[0].get();
		}
		ptr_next->AddChild(std::move(plan));
		plan = std::move(itr_next->second);
	}
	/*
	if (insert_create_table) {
		plan = InsertCreateTable(std::move(plan), (LogicalOperator*)plan_ptr);
	}
	*/
	return plan;
}

/* Will this node be filtered? */
bool PredicateTransferOptimizer::PossibleFilterAny(LogicalOperator &node, bool reverse) {
	// return true;
	if(!reverse) {
		if (node.type == LogicalOperatorType::LOGICAL_GET) {
			auto& get = node.Cast<LogicalGet>();
			if(get.table_filters.filters.size() == 0) {
				return false;
			}
		} else if (node.type == LogicalOperatorType::LOGICAL_UNION) {
			return false;
		}
	} else {
		if (replace_map_forward.find(&node) == replace_map_forward.end()) {
			if (node.type == LogicalOperatorType::LOGICAL_GET) {
				auto& get = node.Cast<LogicalGet>();
				if (get.table_filters.filters.size() == 0) {
					return false;
				}
			} else if (node.type == LogicalOperatorType::LOGICAL_UNION) {
				return false;
			}
		}
	}
	return true;
}

/* Only for microbenchmark */
unique_ptr<LogicalOperator> PredicateTransferOptimizer::InsertCreateTable(unique_ptr<LogicalOperator> plan, LogicalOperator* plan_ptr) {
	if(plan_ptr->type != LogicalOperatorType::LOGICAL_GET && plan_ptr->type != LogicalOperatorType::LOGICAL_FILTER) {
		return plan;
	}
	LogicalGet& get = LogicalGetinFilter(*plan_ptr);
	auto info = make_uniq<CreateTableInfo>();
	info->catalog = INVALID_CATALOG;
	info->schema = DEFAULT_SCHEMA;
	auto name = get.function.to_string(get.bind_data.get());
	if(table_exists.find(name) == table_exists.end()) {
		info->table = name + "1";
		table_exists[name] = 2;
	} else {
		info->table = name + std::to_string(table_exists[name]);
		table_exists[name]++;
	}
	
	if (get.projection_ids.empty()) {
		for (auto &index : get.column_ids) {
			info->columns.AddColumn(ColumnDefinition(get.names[index], get.returned_types[index]));
		}
	} else {
		for (auto &proj_index : get.projection_ids) {
			auto &index = get.column_ids[proj_index];
			info->columns.AddColumn(ColumnDefinition(get.names[index], get.returned_types[index]));
		}
	}
	if (info->catalog.empty() && !info->schema.empty()) {
		auto &db_manager = DatabaseManager::Get(context);
		auto database = db_manager.GetDatabase(context, info->schema);
		if (database) {
			auto schema_obj = Catalog::GetSchema(context, INVALID_CATALOG, info->schema, OnEntryNotFound::RETURN_NULL);
			if (schema_obj) {
				auto &attached = schema_obj->catalog.GetAttached();
				throw BinderException("Ambiguous reference to catalog or schema \"%s\" - use a fully qualified path like \"%s.%s\"", info->schema, attached.GetName(), info->schema);
			}
			info->catalog = info->schema;
			info->schema = string();
		}
	}
	if (IsInvalidCatalog(info->catalog) && info->temporary) {
		info->catalog = TEMP_CATALOG;
	}
	auto &search_path = ClientData::Get(context).catalog_search_path;
	if (IsInvalidCatalog(info->catalog) && IsInvalidSchema(info->schema)) {
		auto &default_entry = search_path->GetDefault();
		info->catalog = default_entry.catalog;
		info->schema = default_entry.schema;
	} else if (IsInvalidSchema(info->schema)) {
		info->schema = search_path->GetDefaultSchema(info->catalog);
	} else if (IsInvalidCatalog(info->catalog)) {
		info->catalog = search_path->GetDefaultCatalog(info->schema);
	}
	if (IsInvalidCatalog(info->catalog)) {
		info->catalog = DatabaseManager::GetDefaultDatabase(context);
	}
	if (!info->temporary) {
		// non-temporary create: not read only
		if (info->catalog == TEMP_CATALOG) {
			throw ParserException("Only TEMPORARY table names can use the \"%s\" catalog", TEMP_CATALOG);
		}
	} else {
		if (info->catalog != TEMP_CATALOG) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" catalog", TEMP_CATALOG);
		}
	}
	auto &schema_obj = Catalog::GetSchema(context, info->catalog, info->schema);
	D_ASSERT(schema_obj.type == CatalogType::SCHEMA_ENTRY);
	info->schema = schema_obj.name;
	auto bound_info = make_uniq<BoundCreateTableInfo>(schema_obj, std::move(info));
	auto &schema = bound_info->schema;
	auto create_table = make_uniq<LogicalCreateTableforBF>(schema, std::move(bound_info));
	create_table->AddChild(std::move(plan));
	return unique_ptr_cast<LogicalCreateTableforBF, LogicalOperator>(std::move(create_table));
}
}