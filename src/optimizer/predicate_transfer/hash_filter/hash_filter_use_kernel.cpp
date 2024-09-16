#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter_use_kernel.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

void HashFilterUseKernel::filter(const vector<Vector> &result,
            shared_ptr<HashFilter> hash_filter,
            SelectionVector &sel,
            idx_t &approved_tuple_count,
            idx_t row_num) {
    if (hash_filter->isEmpty()) {
        approved_tuple_count = 0;
        return;
    }
    idx_t result_count = 0;
    auto& temp = const_cast<Vector &>(result[hash_filter->BoundColsApplied[0]]);
    temp.Flatten(row_num);
    hash_filter->Find(arrow::internal::CpuInfo::AVX2, row_num, reinterpret_cast<int32_t *>(temp.GetData()), sel, result_count, false);

    approved_tuple_count = result_count;
    return;
}
}