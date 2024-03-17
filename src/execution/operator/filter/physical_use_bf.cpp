#include "duckdb/execution/operator/filter/physical_use_bf.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.hpp"

namespace duckdb {
PhysicalUseBF::PhysicalUseBF(vector<LogicalType> types, vector<BlockedBloomFilter*> bf, idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::USE_BF, std::move(types), estimated_cardinality), bf_to_use(bf) {}
    
unique_ptr<OperatorState> PhysicalUseBF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<CachingOperatorState>();
}

OperatorResultType PhysicalUseBF::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<CachingOperatorState>();
	idx_t row_num = input.size();
    idx_t result_count = input.size();
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < result_count; i++) {
		sel.set_index(i, i);
	}
    for (auto itr = bf_to_use.begin(); itr != bf_to_use.end(); itr++) {
		auto bf = *itr;
        Vector &result = input.data[bf->Ref];
		BloomFilterUseKernel::filter(result, bf, sel, result_count, row_num);
    }
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalUseBF::ParamsToString() const {
	string result;
	return result;
}
}