#pragma once

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_create_abstract_kernel.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {

/**
 * Op to add elements to an empty bloom filter created by BloomFilterCreatePreparePOp
 */
class BloomFilterCreateKernel: public BloomFilterCreateAbstractKernel {

public:
  // BloomFilterCreateKernel(const std::vector<std::string> &columnNames, double desiredFalsePositiveRate);
  BloomFilterCreateKernel(double desiredFalsePositiveRate);
  BloomFilterCreateKernel() = default;
  BloomFilterCreateKernel(const BloomFilterCreateKernel&) = default;
  BloomFilterCreateKernel& operator=(const BloomFilterCreateKernel&) = default;
  ~BloomFilterCreateKernel() override = default;
  
  // static std::shared_ptr<BloomFilterCreateKernel> make(const std::vector<std::string> &columnNames,
  //                                                      double desiredFalsePositiveRate);
  
  static std::shared_ptr<BloomFilterCreateKernel> make(double desiredFalsePositiveRate);
  
  double getDesiredFalsePositiveRate() const;

  bool InitBloomFilter(ColumnBinding col_binding, double cardinality) override;
  bool BuildBloomFilter(DataChunk &data_chunk, idx_t col_idx) override;
  std::shared_ptr<BloomFilterBase> getBloomFilter() const override;

  void clear() override;

private:
  double desiredFalsePositiveRate_;

  std::shared_ptr<BloomFilter> bloomFilter_;
};
}