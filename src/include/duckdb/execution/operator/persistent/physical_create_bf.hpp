#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {
class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_BF;

public:
    PhysicalCreateBF(vector<LogicalType> types, unordered_map<idx_t, vector<BlockedBloomFilter*>> bf, idx_t estimated_cardinality);

    unordered_map<idx_t, vector<BlockedBloomFilter*>> bf_to_create;

	shared_ptr<Pipeline> this_pipeline;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const;

	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context, OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	string ParamsToString() const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	void BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline);

private:
    idx_t counter = 0;

    // unordered_map<idx_t, vector<shared_ptr<BloomFilterBuilder_SingleThreaded>>> builders;
};
}