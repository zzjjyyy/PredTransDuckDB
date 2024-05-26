#pragma once

#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {
class LogicalCreateTableforBF : public LogicalCreateTable {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_TABLE_BF;

public:
	LogicalCreateTableforBF(SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info);

    vector<ColumnBinding> GetColumnBindings() override;
    
protected:
	void ResolveTypes() override;
};
}