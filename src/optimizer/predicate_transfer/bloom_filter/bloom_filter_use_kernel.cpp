#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
namespace duckdb {

void BloomFilterUseKernel::filter(const Vector &result,
            BlockedBloomFilter* bloom_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            ValidityMask &mask) {
    SelectionVector new_sel(approved_tuple_count);
    idx_t result_count = 0;
    Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(const_cast<Vector &>(result), hashes, mask.TargetCount());
    // Here can be SIMD ? 
    for (auto i = 0; i < approved_tuple_count; i++) {
        auto idx = sel.get_index(i);
		if (mask.RowIsValid(idx) && bloom_filter->Find(hashes.GetValue(idx).GetValue<hash_t>())) {
			new_sel.set_index(result_count++, idx);
		}
    }
    approved_tuple_count = result_count;
    sel.Initialize(new_sel);
    return;
}
}