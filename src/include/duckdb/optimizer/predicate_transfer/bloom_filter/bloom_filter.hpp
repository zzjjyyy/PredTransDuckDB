#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include "duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter_base.hpp"
#include "duckdb/optimizer/predicate_transfer/bloom_filter/universal_hash_function.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {
#define BLOOM_FILTER_MAX_INPUT_SIZE 20000000

class BloomFilter: public BloomFilterBase {

public:
  inline static constexpr double DefaultDesiredFalsePositiveRate = 0.01;

  BloomFilter(ColumnBinding col, int64_t capacity, double falsePositiveRate);
  static std::shared_ptr<BloomFilter> make(ColumnBinding col, int64_t capacity, double falsePositiveRate);

  /**
   * Used when reconstructing at store during pushdown
   */
  BloomFilter(ColumnBinding col, 
              int64_t capacity,
              double falsePositiveRate,
              bool valid,
              int64_t numHashFunctions,
              int64_t numBits,
              const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
              const std::vector<int64_t> &bitArray);
  static std::shared_ptr<BloomFilter> make(ColumnBinding col, 
                                           int64_t capacity,
                                           double falsePositiveRate,
                                           bool valid,
                                           int64_t numHashFunctions,
                                           int64_t numBits,
                                           const std::vector<std::shared_ptr<UniversalHashFunction>> &hashFunctions,
                                           const std::vector<int64_t> &bitArray);

  BloomFilter() = default;
  BloomFilter(const BloomFilter&) = default;
  BloomFilter& operator=(const BloomFilter&) = default;
  ~BloomFilter() override = default;

  void init();
  void add(int64_t key);
  bool contains(int64_t key);

  ColumnBinding GetCol();

  std::shared_ptr<BloomFilter> CopywithNewColBinding(ColumnBinding column_binding);

  const std::vector<int64_t> getBitArray() const;
  void setBitArray(const std::vector<int64_t> &bitArray);

  /**
   * Merge another bloom filter into this
   * @param other
   */
  bool merge(const std::shared_ptr<BloomFilter> &other);

private:
  ColumnBinding column_binding;

  double falsePositiveRate_;

  int64_t numHashFunctions_;
  int64_t numBits_;

  std::vector<std::shared_ptr<UniversalHashFunction>> hashFunctions_;
  std::vector<int64_t> bitArray_;

  int64_t calculateNumHashFunctions() const;
  int64_t calculateNumBits() const;

  std::vector<std::shared_ptr<UniversalHashFunction>> makeHashFunctions();
  std::vector<int64_t> makeBitArray() const;

  std::vector<int64_t> hashes(int64_t key);

  /**
   * Conversion formulas below
   *
   * n Capacity
   * p False positive rate
   * k Number of hash functions
   * m Number of bits
   */

  /**
   *
   * @param p False positive rate
   * @return k Number of hash functions
   */
  static int64_t k_from_p(double p);

  /**
   *
   * @param n Capacity
   * @param p False positive rate
   * @return m Number of bits
   */
  static int64_t m_from_np(int64_t n, double p);

// caf inspect (currently bloom filter is never sent across compute nodes, so this is never called)
public:
  template <class Inspector>
  friend bool inspect(Inspector& f, BloomFilter& bf) {
    return f.object(bf).fields(f.field("type", bf.type_),
                               f.field("capacity", bf.capacity_),
                               f.field("valid", bf.valid_),
                               f.field("falsePositiveRate", bf.falsePositiveRate_));
  }
};

}


#endif //FPDB_FPDB_EXECUTOR_INCLUDE_FPDB_EXECUTOR_PHYSICAL_BLOOMFILTER_BLOOMFILTER_H
