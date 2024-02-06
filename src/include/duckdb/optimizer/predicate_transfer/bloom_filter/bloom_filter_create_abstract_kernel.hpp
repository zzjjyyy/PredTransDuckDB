#pragma once

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_base.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/column_binding.hpp"
#include <vector>
#include <string>

namespace duckdb {

enum BloomFilterCreateKernelType {
  VANILLA_KERNEL,
  ARROW_KERNEL,
  GLOBAL_ARROW_KERNEL
};

class BloomFilterCreateAbstractKernel {
public:
  // BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType type, const std::vector<std::string> &columnNames)
  //  : type_(type), columnNames_(columnNames) {};
  BloomFilterCreateAbstractKernel(BloomFilterCreateKernelType type)
    : type_(type) {};
  BloomFilterCreateAbstractKernel() = default;
  BloomFilterCreateAbstractKernel(const BloomFilterCreateAbstractKernel&) = default;
  BloomFilterCreateAbstractKernel& operator=(const BloomFilterCreateAbstractKernel&) = default;
  virtual ~BloomFilterCreateAbstractKernel() = default;

  BloomFilterCreateKernelType getType() const;

  virtual bool InitBloomFilter(ColumnBinding col_binding, double cardinality) = 0;
  virtual bool BuildBloomFilter(DataChunk &data_chunk, idx_t col_idx) = 0;
  virtual std::shared_ptr<BloomFilterBase> getBloomFilter() const = 0;

  virtual void clear();

protected:
  BloomFilterCreateKernelType type_;
  // std::vector<std::string> columnNames_;
};
}