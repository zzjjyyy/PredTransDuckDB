#pragma once

#include <string>

namespace duckdb {

enum BloomFilterType {
  VANILLA_BF,
  ARROW_BF,
  GLOBAL_ARROW_BF
};

class BloomFilterBase {
public:
  BloomFilterBase(BloomFilterType type, int64_t capacity, bool valid);
  BloomFilterBase() = default;
  BloomFilterBase(const BloomFilterBase&) = default;
  BloomFilterBase& operator=(const BloomFilterBase&) = default;
  virtual ~BloomFilterBase() = default;

  BloomFilterType getType() const;
  bool valid() const;

protected:
  BloomFilterType type_;
  int64_t capacity_;
  bool valid_;    // whether this bloom filter will be used after runtime checking (i.e. false if input is too large)
};
}