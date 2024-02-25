#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class LogicalCreateBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_BF;

public:
    LogicalCreateBF(std::unordered_map<duckdb::idx_t, duckdb::BlockedBloomFilter *> &temp_result);

    std::unordered_map<duckdb::idx_t, duckdb::BlockedBloomFilter *> &bf_to_create;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
}