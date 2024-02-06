//
// Created by Yifei Yang on 11/23/22.
//

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_base.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"

namespace duckdb {

BloomFilterBase::BloomFilterBase(BloomFilterType type, int64_t capacity, bool valid):
  type_(type),
  capacity_(capacity),
  valid_(valid) {}

BloomFilterType BloomFilterBase::getType() const {
  return type_;
}

bool BloomFilterBase::valid() const {
  return valid_;
}
}