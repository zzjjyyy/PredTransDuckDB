#include "duckdb/planner/operator/logical_create_table_for_BF.hpp"

namespace duckdb {
LogicalCreateTableforBF::LogicalCreateTableforBF(SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : LogicalCreateTable(schema, std::move(info)) {  
}

vector<ColumnBinding> LogicalCreateTableforBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateTableforBF::ResolveTypes() {
	types = children[0]->types;
}
}