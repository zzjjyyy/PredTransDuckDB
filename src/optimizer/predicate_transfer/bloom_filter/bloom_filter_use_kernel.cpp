#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

void BloomFilterUseKernel::filter(const Vector &result,
            const std::shared_ptr<BloomFilter> &bloom_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            ValidityMask &mask) {
    SelectionVector new_sel(approved_tuple_count);
    idx_t result_count = 0;
    for (auto i = 0; i < approved_tuple_count; i++) {
        auto idx = sel.get_index(i);
		if (mask.RowIsValid(idx) && bloom_filter->contains(result.GetValue(i).Hash())) {
			new_sel.set_index(result_count++, idx);
		}
    }
    approved_tuple_count = result_count;
    sel.Initialize(new_sel);
    return;
}
}