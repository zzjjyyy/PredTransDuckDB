#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
namespace duckdb {

void BloomFilterUseKernel::filter(const Vector &result,
            BlockedBloomFilter* bloom_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            idx_t row_num) {
    SelectionVector new_sel(approved_tuple_count);
    if (bloom_filter->isEmpty()) {
        sel.Initialize(new_sel);
        approved_tuple_count = 0;
        return;
    }
    idx_t result_count = 0;
    Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(const_cast<Vector &>(result), hashes, row_num);

    bloom_filter->Find(arrow::internal::CpuInfo::AVX2, row_num,
                       (hash_t*)hashes.GetData(), sel, new_sel, result_count, true);

    approved_tuple_count = result_count;
    sel.Initialize(new_sel);
    return;
}
}