#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class LogicalUseBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_USE_BF;

public:
    LogicalUseBF(vector<duckdb::BlockedBloomFilter*> temp_result);

    vector<duckdb::BlockedBloomFilter*> bf_to_use;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
}