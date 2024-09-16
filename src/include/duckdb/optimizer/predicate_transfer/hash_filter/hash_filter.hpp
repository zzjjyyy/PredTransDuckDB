#pragma once

#include <immintrin.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_set>

#include "arrow/acero/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

// A Hash filter implementation.
class HashFilter {
  friend class HashFilterBuilder_SingleThreaded;
  friend class HashFilterBuilder_Parallel;

public:
  HashFilter()
    : Used_(false) {}
  HashFilter(int log_num_blocks, bool use_64bit_hashes)
    : Used_(false) {}

  void AddColumnBindingApplied(ColumnBinding column_binding) {
    column_bindings_applied_.emplace_back(column_binding);
  }

  void AddColumnBindingBuilt(ColumnBinding column_binding) {
    column_bindings_built_.emplace_back(column_binding);
  }

  inline bool Find(int32_t value) const {
    return hash_set.find(value) != hash_set.end();
  }

  void Find(int64_t hardware_flags, int64_t num_rows, int32_t* values,
            SelectionVector &sel, idx_t &result_count, bool enable_prefetch = true) const;

  vector<ColumnBinding> GetColApplied() {
    return column_bindings_applied_;
  }

  vector<ColumnBinding> GetColBuilt() {
    return column_bindings_built_;
  }

  bool isEmpty() {
    return hash_set.size() == 0;
  }
  
  bool isUsed() {
    return Used_;
  }
  
  void setUsed() {
    Used_ = true;
  }
 
  vector<idx_t> BoundColsApplied;

  vector<idx_t> BoundColsBuilt;
  
  arrow::Status CreateEmpty(int64_t num_rows_to_insert);

  void Insert(int64_t hardware_flags, int64_t num_rows, int32_t* value);

private:
  struct Hash{                                          
      size_t operator()(const Value& rhs)const {       
          return rhs.Hash();
      }
  };

  struct EqualTo{
      bool operator()(const Value& lhs, const Value& rhs)const{
          return lhs == rhs;     
      }
  };

  inline void Insert(int32_t value) {
    hash_set.insert(value);
  }

  inline void InsertImp(int64_t num_rows, int32_t* values);

  inline void FindImp(int64_t num_rows, int32_t* values, SelectionVector &sel,
                      idx_t &result_count, bool enable_prefetch) const;
  
  // The columns applied this Hash Filter
  vector<ColumnBinding> column_bindings_applied_;
  
  // The columns build this Hash Filter
  vector<ColumnBinding> column_bindings_built_;

  // Buffer allocated to store an array of power of 2 64-bit blocks.
  std::shared_ptr<arrow::Buffer> buf_;
  
  // the key component of hash filter
  std::unordered_set<int32_t> hash_set;
  // std::unordered_set<Value, Hash, EqualTo> hash_set;

  bool Used_;
};

// We have two separate implementations of building a Hash filter, multi-threaded and
// single-threaded.
enum class HashFilterBuildStrategy {
  SINGLE_THREADED = 0,
  PARALLEL = 1,
};

class HashFilterBuilder {
public:
  virtual ~HashFilterBuilder() = default;
  virtual arrow::Status Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool* pool,
                              int64_t num_rows, int64_t num_batches,
                              HashFilter* build_target) = 0;

  virtual int64_t num_tasks() const { return 0; }
  virtual arrow::Status PushNextBatch(size_t thread_index, int64_t num_rows, int32_t* values) = 0;

  virtual vector<idx_t> BuiltCols() = 0;

  virtual void CleanUp() {}
  virtual void Print() = 0;
  virtual void Merge() {}
};

class HashFilterBuilder_SingleThreaded : public HashFilterBuilder {
public:
  arrow::Status Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool* pool,
                      int64_t num_rows, int64_t num_batches,
                      HashFilter* build_target) override;

  arrow::Status PushNextBatch(size_t /*thread_index*/, int64_t num_rows, int32_t* values) override;

  vector<idx_t> BuiltCols() override;
  void Print();
private:
  void PushNextBatchImp(int64_t num_rows, int32_t* values);
  
  int64_t hardware_flags_;
  HashFilter* build_target_;
};

class ARROW_ACERO_EXPORT HashFilterBuilder_Parallel : public HashFilterBuilder {
  public:
    arrow::Status Begin(size_t num_threads, int64_t hardware_flags, arrow::MemoryPool* pool,
                        int64_t num_rows, int64_t num_batches,
                        HashFilter* build_target) override;

    arrow::Status PushNextBatch(size_t thread_id, int64_t num_rows, int32_t* values) override;

    void CleanUp() override;

    void Merge() override;

    vector<idx_t> BuiltCols() override;
    void Print();
  private:
    void PushNextBatchImp(size_t thread_id, int64_t num_rows, int32_t* values);

  int64_t total_row_nums_;
  int64_t hardware_flags_;
  HashFilter* build_target_;
  int log_num_prtns_;
};
}