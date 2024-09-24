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
#ifdef UseHashFilter
	LogicalCreateBF(vector<shared_ptr<HashFilter>> temp_result);
#else
	LogicalCreateBF(vector<shared_ptr<BlockedBloomFilter>> temp_result);
#endif

#ifdef UseHashFilter
	vector<shared_ptr<HashFilter>> bf_to_create;
#else  
	vector<shared_ptr<BlockedBloomFilter>> bf_to_create;
#endif

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
}