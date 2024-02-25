#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class PhysicalCreateBF : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_BF;

public:
    PhysicalCreateBF(vector<LogicalType> types, unordered_map<idx_t, BlockedBloomFilter*> &bf, idx_t estimated_cardinality, idx_t num_threads);

    unordered_map<idx_t, BlockedBloomFilter*> &bf_to_create;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	string ParamsToString() const override;

protected:
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

private:
    idx_t counter = 0;

    unordered_map<idx_t, shared_ptr<BloomFilterBuilder_SingleThreaded>> builders;
};
}