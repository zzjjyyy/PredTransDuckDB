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
    Vector temp(const_cast<Vector &>(result), sel, approved_tuple_count);
	VectorOperations::Hash(temp, hashes, approved_tuple_count);

    uint8_t* result_bit_vector = new uint8_t[approved_tuple_count / 8 + 1];
    bloom_filter->Find(arrow::internal::CpuInfo::AVX2, approved_tuple_count, (hash_t*)hashes.GetData(), result_bit_vector);
    for (auto i = 0; i < approved_tuple_count; i++) {
        auto idx = sel.get_index(i);
        uint8_t result_byte = result_bit_vector[i / 8];
        uint8_t result_bit = result_byte & (1 << (i % 8));
		if (result_bit != 0) {
			new_sel.set_index(result_count++, idx);
		}
    }
    delete[] result_bit_vector;
    approved_tuple_count = result_count;
    sel.Initialize(new_sel);
    return;
}
}