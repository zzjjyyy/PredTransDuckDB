#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"

namespace duckdb {
PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, unordered_map<idx_t, vector<BlockedBloomFilter*>> bf, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CREATE_BF, std::move(types), estimated_cardinality), bf_to_create(bf) {
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateBFGlobalState : public GlobalSinkState {
public:
	CreateBFGlobalState(ClientContext &context, const PhysicalCreateBF &op)
		: data(context, op.types) {}

	mutex glock;
	ColumnDataCollection data;
};

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.global_state.Cast<CreateBFGlobalState>();
	chunk.Flatten();
	/*
	for(auto &filter_builder_vec : builders) {
		for(auto &filter_builder : filter_builder_vec.second) {
			Vector hashes(LogicalType::HASH);
			VectorOperations::Hash(chunk.data[filter_builder_vec.first], hashes, chunk.size());
			filter_builder->PushNextBatch(arrow::internal::CpuInfo::AVX2, chunk.size(), (hash_t*)hashes.GetData());
		}
	}
	*/
	state.data.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  		OperatorSinkFinalizeInput &input) const {	
	auto &state = input.global_state.Cast<CreateBFGlobalState>();
	int64_t num_rows = state.data.Count();
	unordered_map<idx_t, vector<shared_ptr<BloomFilterBuilder_SingleThreaded>>> builders;
	for (auto &filter_vec : bf_to_create) {
		for(auto &filter : filter_vec.second) {
			auto builder = make_shared<BloomFilterBuilder_SingleThreaded>();
			builder->Begin(1, arrow::internal::CpuInfo::AVX2, arrow::MemoryPool::CreateDefault().get(),
						   num_rows, 0, filter);
			builders[filter_vec.first].emplace_back(builder);
		}
	}
	for (auto &chunk : state.data.Chunks()) {
		for(auto &filter_builder_vec : builders) {
			for(auto &filter_builder : filter_builder_vec.second) {
				Vector hashes(LogicalType::HASH);
				VectorOperations::Hash(chunk.data[filter_builder_vec.first], hashes, chunk.size());
				filter_builder->PushNextBatch(arrow::internal::CpuInfo::AVX2, chunk.size(), (hash_t*)hashes.GetData());
			}
		}
	}
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateBFGlobalState>(context, *this);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateBFSourceState : public GlobalSourceState {
public:
	CreateBFSourceState(const PhysicalCreateBF &op) {
		current_offset = 0;
		D_ASSERT(op.sink_state);
		auto &gstate = op.sink_state->Cast<CreateBFGlobalState>();
		gstate.data.InitializeScan(scan_state);
	}

	idx_t current_offset;
	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalCreateBF::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CreateBFSourceState>(*this);
}

SourceResultType PhysicalCreateBF::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CreateBFGlobalState>();
	auto &state = input.global_state.Cast<CreateBFSourceState>();
	if (!gstate.data.Scan(state.scan_state, chunk)) {
		return SourceResultType::FINISHED;
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalCreateBF::ParamsToString() const {
	string result;
	return result;
}

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	// operator is a sink, build a pipeline
	D_ASSERT(children.size() == 1);

	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(*children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}

/* Add related createBF dependency */
void PhysicalCreateBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);
	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(*children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}
}