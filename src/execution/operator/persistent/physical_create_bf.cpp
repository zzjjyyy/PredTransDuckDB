#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, unordered_map<idx_t, BlockedBloomFilter*> &bf, idx_t estimated_cardinality, idx_t num_threads)
    : CachingPhysicalOperator(PhysicalOperatorType::CREATE_BF, std::move(types), estimated_cardinality), bf_to_create(bf) {
        for (auto &filter : bf_to_create) {
			auto builder = make_shared<BloomFilterBuilder_SingleThreaded>();
			builder->Begin(1, arrow::internal::CpuInfo::AVX2, arrow::MemoryPool::CreateDefault().get(),
						   this->estimated_cardinality, 1000, filter.second);
			builders[filter.first] = builder;
		}
    };

unique_ptr<OperatorState> PhysicalCreateBF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CachingOperatorState>();
}

OperatorResultType PhysicalCreateBF::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<OperatorState>();
    for(auto &filter_builder : builders) {
		Vector hashes(LogicalType::HASH);
		VectorOperations::Hash(input.data[filter_builder.first], hashes, input.size());
		filter_builder.second->PushNextBatch(arrow::internal::CpuInfo::AVX2, input.size(), (hash_t*)hashes.GetData());
	}
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalCreateBF::ParamsToString() const {
	string result;
	return result;
}
}