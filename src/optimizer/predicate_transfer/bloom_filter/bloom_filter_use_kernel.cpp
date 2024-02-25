#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
namespace duckdb {

void BloomFilterUseKernel::filter(const Vector &result,
            BlockedBloomFilter* bloom_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            ValidityMask &mask) {
    idx_t row_nums = mask.TargetCount();
    SelectionVector new_sel(approved_tuple_count);
    if (bloom_filter->isEmpty()) {
        sel.Initialize(new_sel);
        approved_tuple_count = 0;
        return;
    }
    idx_t result_count = 0;
    Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(const_cast<Vector &>(result), hashes, row_nums);
    /*
    for (auto i = 0; i < approved_tuple_count; i++) {
        auto idx = sel.get_index(i);
		if (mask.RowIsValid(idx) && bloom_filter->Find(hashes.GetValue(idx).GetValue<hash_t>())) {
			new_sel.set_index(result_count++, idx);
		}
    }
    */
    uint8_t* result_bit_vector = new uint8_t[row_nums / 8 + 1];
    bloom_filter->Find(1, row_nums, (hash_t*)hashes.GetData(), result_bit_vector);
    for (auto i = 0; i < approved_tuple_count; i++) {
        auto idx = sel.get_index(i);
        uint8_t result_byte = result_bit_vector[idx / 8];
        uint8_t result_bit = result_byte & (1 << (idx % 8));
		if (mask.RowIsValid(idx) && result_bit != 0) {
			new_sel.set_index(result_count++, idx);
		}
    }
    delete[] result_bit_vector;
    if(result_count == 0) {
        result_count = 0;
    }
    approved_tuple_count = result_count;
    sel.Initialize(new_sel);
    return;
}
}