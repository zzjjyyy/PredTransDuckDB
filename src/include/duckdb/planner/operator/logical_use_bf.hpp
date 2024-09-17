#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {
class LogicalUseBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_USE_BF;

public:
	/* Hash Filter or Bloom Filter */
	// LogicalUseBF(vector<shared_ptr<HashFilter>> temp_result);
    LogicalUseBF(vector<shared_ptr<BlockedBloomFilter>> temp_result);

	/* Hash Filter or Bloom Filter */
	// vector<shared_ptr<HashFilter>> bf_to_use;
    vector<shared_ptr<BlockedBloomFilter>> bf_to_use;

	vector<LogicalCreateBF*> related_create_bf;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

	vector<ColumnBinding> GetColumnBindings() override;

	void AddDownStreamOperator(LogicalCreateBF *op);

protected:
	void ResolveTypes() override;
};
}