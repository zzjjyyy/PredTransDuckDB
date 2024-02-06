#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp"
#include <fmt/format.h>
#include <cmath>

namespace duckdb {

BloomFilter::BloomFilter(ColumnBinding col, int64_t capacity, double falsePositiveRate) :
  BloomFilterBase(BloomFilterType::VANILLA_BF, capacity, capacity <= BLOOM_FILTER_MAX_INPUT_SIZE),
  falsePositiveRate_(falsePositiveRate), column_binding(col) {
  D_ASSERT(falsePositiveRate >= 0.0 && falsePositiveRate <= 1.0);
}

BloomFilter::BloomFilter(ColumnBinding col,
                         int64_t capacity,
                         double falsePositiveRate,
                         bool valid,
                         int64_t numHashFunctions,
                         int64_t numBits,
                         const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
                         const std::vector<int64_t> &bitArray) :
  BloomFilterBase(BloomFilterType::VANILLA_BF, capacity, valid),
  column_binding(col),
  falsePositiveRate_(falsePositiveRate),
  numHashFunctions_(numHashFunctions),
  numBits_(numBits),
  hashFunctions_(hashFunctions),
  bitArray_(bitArray) {

  D_ASSERT(falsePositiveRate >= 0.0 && falsePositiveRate <= 1.0);
}

std::shared_ptr<BloomFilter> BloomFilter::make(ColumnBinding col, int64_t capacity, double falsePositiveRate) {
  return std::make_shared<BloomFilter>(col, capacity, falsePositiveRate);
}

std::shared_ptr<BloomFilter> BloomFilter::make(ColumnBinding col,
                                               int64_t capacity,
                                               double falsePositiveRate,
                                               bool valid,
                                               int64_t numHashFunctions,
                                               int64_t numBits,
                                               const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
                                               const std::vector<int64_t> &bitArray) {
  return std::make_shared<BloomFilter>(col, capacity, falsePositiveRate, valid,
                                       numHashFunctions, numBits, hashFunctions, bitArray);
}

void BloomFilter::init() {
  numHashFunctions_ = calculateNumHashFunctions();
  numBits_ = calculateNumBits();
  hashFunctions_ = makeHashFunctions();
  bitArray_ = makeBitArray();
}

void BloomFilter::add(int64_t key) {
  D_ASSERT(capacity_ > 0);

  auto hs = hashes(key);

  for (auto h: hs) {
    // set h-th bit
    int64_t valueId = h / 64;
    int valueOffset = h % 64;
    bitArray_[valueId] ^= (-1 ^ bitArray_[valueId]) & (1UL << valueOffset);
  }
}

bool BloomFilter::contains(int64_t key) {
  if (capacity_ == 0)
    return false;

  auto hs = hashes(key);

  for (auto h: hs) {
    // check h-th bit
    int64_t valueId = h / 64;
    int valueOffset = h % 64;
    if (!((bitArray_[valueId] >> valueOffset) & 1UL)) {
      return false;
    }
  }

  return true;
}

ColumnBinding BloomFilter::GetCol() {
  return column_binding;
}

std::shared_ptr<BloomFilter> BloomFilter::CopywithNewColBinding(ColumnBinding column_binding){
  return BloomFilter::make(column_binding, capacity_, falsePositiveRate_, valid_, numHashFunctions_, numBits_, hashFunctions_, bitArray_);
}

const std::vector<int64_t> BloomFilter::getBitArray() const {
  return bitArray_;
}

void BloomFilter::setBitArray(const std::vector<int64_t> &bitArray) {
  bitArray_ = bitArray;
};

bool BloomFilter::merge(const std::shared_ptr<BloomFilter> &other) {
  // check
  if (capacity_ != other->capacity_) {
    throw InternalException("Capacity mismatch, %d vs %d", capacity_, other->capacity_);
  }
  if (falsePositiveRate_ != other->falsePositiveRate_) {
    throw InternalException("FalsePositiveRate mismatch, %d vs %d",
                             falsePositiveRate_, other->falsePositiveRate_);
  }

  // update bit arrays
  std::vector<int64_t> mergedBitArray;
  mergedBitArray.reserve(bitArray_.size());
  for (uint64_t i = 0; i < bitArray_.size(); ++i) {
    mergedBitArray.emplace_back(bitArray_[i] | other->bitArray_[i]);
  }
  bitArray_ = mergedBitArray;

  return true;
}

int64_t BloomFilter::calculateNumHashFunctions() const {
  return k_from_p(falsePositiveRate_);
}

int64_t BloomFilter::calculateNumBits() const {
  return m_from_np(capacity_, falsePositiveRate_);
}

std::vector<std::shared_ptr<UniversalHashFunction>> BloomFilter::makeHashFunctions() {
  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions;

  // check capacity
  if (capacity_ == 0) {
    numHashFunctions_ = 0;
    return hashFunctions;
  }

  hashFunctions.reserve(numHashFunctions_);
  for (int64_t s = 0; s < numHashFunctions_; ++s) {
    hashFunctions.emplace_back(UniversalHashFunction::make(numBits_));
  }
  return hashFunctions;
}

std::vector<int64_t> BloomFilter::makeBitArray() const {
  int64_t len = numBits_ / 64 + 1;
  return std::vector<int64_t>(len, 0);
}

std::vector<int64_t> BloomFilter::hashes(int64_t key) {
  std::vector<int64_t> hashes(hashFunctions_.size());

  for (size_t i = 0; i < hashFunctions_.size(); ++i) {
    hashes[i] = hashFunctions_[i]->hash(key);
  }

  return hashes;
}

int64_t BloomFilter::k_from_p(double p) {
  return std::ceil(
          std::log(1 / p) / std::log(2));
}

int64_t BloomFilter::m_from_np(int64_t n, double p) {
  return std::ceil(
          ((double) n) * std::abs(std::log(p)) / std::pow(std::log(2), 2));
}

}
