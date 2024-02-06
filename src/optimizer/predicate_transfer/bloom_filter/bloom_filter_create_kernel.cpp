//
// Created by Yifei Yang on 3/17/22.
//

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_create_kernel.hpp"
#include "duckdb/common/types/hash.hpp"

namespace duckdb {
// BloomFilterCreateKernel::BloomFilterCreateKernel(const std::vector<std::string> &columnNames,
//                                                  double desiredFalsePositiveRate):
//   BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType::VANILLA_KERNEL, columnNames),
//   desiredFalsePositiveRate_(desiredFalsePositiveRate) {}
BloomFilterCreateKernel::BloomFilterCreateKernel(double desiredFalsePositiveRate)
  : BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType::VANILLA_KERNEL), desiredFalsePositiveRate_(desiredFalsePositiveRate) {}

// std::shared_ptr<BloomFilterCreateKernel> BloomFilterCreateKernel::make(const std::vector<std::string> &columnNames,
//                                                                        double desiredFalsePositiveRate) {
//   return std::make_shared<BloomFilterCreateKernel>(columnNames, desiredFalsePositiveRate);
// }

std::shared_ptr<BloomFilterCreateKernel> BloomFilterCreateKernel::make(double desiredFalsePositiveRate) {
  return std::make_shared<BloomFilterCreateKernel>(desiredFalsePositiveRate);
}

double BloomFilterCreateKernel::getDesiredFalsePositiveRate() const {
  return desiredFalsePositiveRate_;
}

bool BloomFilterCreateKernel::InitBloomFilter(ColumnBinding col_binding, double cardinality_p) {
    bloomFilter_ = std::make_shared<BloomFilter>(col_binding, cardinality_p, desiredFalsePositiveRate_);
    if (!bloomFilter_->valid()) {
        return {};
    }
    bloomFilter_->init();
    return true;
}

bool BloomFilterCreateKernel::BuildBloomFilter(DataChunk &data_chunk_p, idx_t col_idx) {
    auto type = data_chunk_p.data[col_idx].GetType();
    for(auto r = 0; r < data_chunk_p.size(); r++) {
        int64_t hash = 0;
        hash = data_chunk_p.GetValue(col_idx, r).Hash();
        bloomFilter_->add(hash);
    }
    return true;
}

std::shared_ptr<BloomFilterBase> BloomFilterCreateKernel::getBloomFilter() const {
  return bloomFilter_;
}

void BloomFilterCreateKernel::clear() {
  BloomFilterCreateAbstractKernel::clear();
  bloomFilter_.reset();
}
}