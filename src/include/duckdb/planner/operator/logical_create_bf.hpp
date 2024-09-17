#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

namespace duckdb {
class LogicalCreateBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_BF;

	PhysicalCreateBF *physical = nullptr;

public:
	/* Hash Filter or Bloom Filter */
	// LogicalCreateBF(vector<shared_ptr<HashFilter>> temp_result);
    LogicalCreateBF(vector<shared_ptr<BlockedBloomFilter>> temp_result);

	/* Hash Filter or Bloom Filter */
	// vector<shared_ptr<HashFilter>> bf_to_create;
    vector<shared_ptr<BlockedBloomFilter>> bf_to_create;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
}