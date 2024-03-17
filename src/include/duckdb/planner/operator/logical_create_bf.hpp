#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

namespace duckdb {
class LogicalCreateBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_BF;

	vector<LogicalCreateBF *> related_create_bf;
	
	PhysicalCreateBF *physical = nullptr;

public:
    LogicalCreateBF(std::unordered_map<duckdb::idx_t, vector<duckdb::BlockedBloomFilter*>> temp_result);

    std::unordered_map<duckdb::idx_t, vector<duckdb::BlockedBloomFilter*>> bf_to_create;

	std::unordered_map<duckdb::idx_t, vector<duckdb::BlockedBloomFilter*>> bf_to_create_bind;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

	vector<ColumnBinding> GetColumnBindings() override;

	void AddDownStreamCreateBF(LogicalCreateBF *op);

protected:
	void ResolveTypes() override;
};
}