#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

void HashFilterUseKernel::filter(vector<Vector> &input,
            shared_ptr<HashFilter> hash_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            idx_t row_num) {
    if (hash_filter->isEmpty()) {
        approved_tuple_count = 0;
        return;
    }
    idx_t result_count = 0;
    DataChunk chunk;
    chunk.SetCardinality(row_num);
    for(int i = 0; i < input.size(); i++) {
        Vector v = input[i];
        chunk.data.emplace_back(v);
    }
    hash_filter->Find(arrow::internal::CpuInfo::AVX2, row_num, chunk, sel, result_count, false);

    approved_tuple_count = result_count;
    return;
}
}