#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_create_abstract_kernel.hpp"

namespace duckdb {
BloomFilterCreateKernelType BloomFilterCreateAbstractKernel::getType() const {
  return type_;
}

void BloomFilterCreateAbstractKernel::clear() {
  // receivedTupleSet_.reset();
}

}
