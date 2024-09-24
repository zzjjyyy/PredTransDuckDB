#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter.hpp"

namespace duckdb {
class PhysicalUseBF : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::USE_BF;

public:
#ifdef UseHashFilter
	PhysicalUseBF(vector<LogicalType> types, vector<shared_ptr<HashFilter>> bf, idx_t estimated_cardinality);
#else
	PhysicalUseBF(vector<LogicalType> types, vector<shared_ptr<BlockedBloomFilter>> bf, idx_t estimated_cardinality);
#endif

#ifdef UseHashFilter
	vector<shared_ptr<HashFilter>> bf_to_use;
#else
	vector<shared_ptr<BlockedBloomFilter>> bf_to_use;
#endif

	vector<PhysicalCreateBF *> related_create_bf;

public:
	/* Operator interface */
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

private:
    idx_t counter = 0;
};
}