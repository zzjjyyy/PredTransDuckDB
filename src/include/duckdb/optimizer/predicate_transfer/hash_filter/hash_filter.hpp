#pragma once

#include <immintrin.h>

#include <atomic>
#include <cstdint>
#include <memory>
// #include <unordered_set>

#include "arrow/acero/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/optimizer/predicate_transfer/hash_filter/hashtable.hpp"

namespace duckdb {
class BufferManager;

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

  void Find(int64_t hardware_flags, int64_t num_rows, DataChunk& values,
            SelectionVector &sel, idx_t &result_count, bool enable_prefetch = true) const;

  vector<ColumnBinding> GetColApplied() {
    return column_bindings_applied_;
  }

  vector<ColumnBinding> GetColBuilt() {
    return column_bindings_built_;
  }

  bool isEmpty() {
    return hash_table->Count() == 0;
  }
  
  bool isUsed() {
    return Used_;
  }
  
  void setUsed() {
    Used_ = true;
  }
  
  arrow::Status CreateEmpty(BufferManager* buffer, vector<LogicalType> layouts);

  void Insert(int64_t hardware_flags, int64_t num_rows, int32_t* value);

  vector<idx_t> BoundColsApplied;

  vector<idx_t> BoundColsBuilt;
  
  // the key component of hash filter
  std::shared_ptr<HashTable> hash_table;


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
 
  // The columns applied this Hash Filter
  vector<ColumnBinding> column_bindings_applied_;
  
  // The columns build this Hash Filter
  vector<ColumnBinding> column_bindings_built_;

  // Buffer allocated to store an array of power of 2 64-bit blocks.
  std::shared_ptr<arrow::Buffer> buf_;
  
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
  virtual arrow::Status Begin(size_t num_threads, int64_t hardware_flags, BufferManager* buffer,
                              vector<LogicalType> layouts, int64_t num_batches,
                              HashFilter* build_target) = 0;

  virtual int64_t num_tasks() const { return 0; }
  virtual arrow::Status PushNextBatch(size_t thread_index, int64_t num_rows, DataChunk& values) = 0;

  virtual vector<idx_t> BuiltCols() = 0;

  virtual void CleanUp() {}
  virtual void Print() = 0;
  virtual void Merge() {}

  HashFilter* build_target_;
};

class HashFilterBuilder_SingleThreaded : public HashFilterBuilder {
public:
  arrow::Status Begin(size_t num_threads, int64_t hardware_flags, BufferManager* buffer,
                      vector<LogicalType> layouts, int64_t num_batches,
                      HashFilter* build_target) override;

  arrow::Status PushNextBatch(size_t /*thread_index*/, int64_t num_rows, DataChunk& values) override;

  vector<idx_t> BuiltCols() override;
  void Print();

private:
  void PushNextBatchImp(int64_t num_rows, int32_t* values);
  int64_t hardware_flags_;
};

class ARROW_ACERO_EXPORT HashFilterBuilder_Parallel : public HashFilterBuilder {
  public:
    arrow::Status Begin(size_t num_threads, int64_t hardware_flags, BufferManager* buffer,
                        vector<LogicalType> layouts, int64_t num_batches,
                        HashFilter* build_target) override;

    arrow::Status PushNextBatch(size_t thread_id, int64_t num_rows, DataChunk& values) override;

    void CleanUp() override;

    void Merge() override;

    vector<idx_t> BuiltCols() override;
    void Print();
  private:
    void PushNextBatchImp(size_t thread_id, int64_t num_rows, int32_t* values);

  int64_t total_row_nums_;
  int64_t hardware_flags_;
  
  int log_num_prtns_;
};
}