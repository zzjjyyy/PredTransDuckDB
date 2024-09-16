#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter.hpp"
#include <random>
#include "arrow/acero/util.h"       // PREFETCH
#include "arrow/util/bit_util.h"    // Log2
#include "arrow/util/bitmap_ops.h"  // CountSetBits
#include "arrow/util/config.h"

#include <iostream>

namespace duckdb {
arrow::Status HashFilter::CreateEmpty(int64_t num_rows_to_insert) {
  hash_set.reserve(num_rows_to_insert);
  return arrow::Status::OK();
}

void HashFilter::InsertImp(int64_t num_rows, int32_t* values) {
  for (int64_t i = 0; i < num_rows; ++i) {
    Insert(values[i]);
  }
}

void HashFilter::Insert(int64_t hardware_flags, int64_t num_rows,
                        int32_t* values) {
  InsertImp(num_rows, values);
}

void HashFilter::FindImp(int64_t num_rows, int32_t* values, SelectionVector &sel,
                         idx_t &result_count, bool enable_prefetch) const {
  for (int64_t i = 0; i < num_rows; i++) {
    bool result = Find(values[i]);
    sel.set_index(result_count, i);
    result_count += result;
  }
}

void HashFilter::Find(int64_t hardware_flags, int64_t num_rows, int32_t* values,
                      SelectionVector &sel, idx_t &result_count, bool enable_prefetch) const {
  FindImp(num_rows, values, sel, result_count, enable_prefetch);
}

arrow::Status HashFilterBuilder_SingleThreaded::Begin(size_t /*num_threads*/,
                                                       int64_t hardware_flags, arrow::MemoryPool* pool,
                                                       int64_t num_rows, int64_t /*num_batches*/,
                                                       HashFilter* build_target) {
  hardware_flags_ = hardware_flags;
  build_target_ = build_target;

  RETURN_NOT_OK(build_target->CreateEmpty(num_rows));

  return arrow::Status::OK();
}

arrow::Status HashFilterBuilder_SingleThreaded::PushNextBatch(size_t /*thread_index*/,
                                                        int64_t num_rows,
                                                        int32_t* values) {
  PushNextBatchImp(num_rows, values);
  return arrow::Status::OK();
}

vector<idx_t> HashFilterBuilder_SingleThreaded::BuiltCols() {
  return build_target_->BoundColsBuilt;
}

void HashFilterBuilder_SingleThreaded::PushNextBatchImp(int64_t num_rows, int32_t* values) {
  build_target_->Insert(hardware_flags_, num_rows, values);
}

void HashFilterBuilder_SingleThreaded::Print() {
  for(auto itr = build_target_->hash_set.begin(); itr != build_target_->hash_set.end(); itr++) {
    std::cout << *itr << " ";
  }
  std::cout << std::endl;
}

arrow::Status HashFilterBuilder_Parallel::Begin(size_t num_threads, int64_t hardware_flags,
                                                 arrow::MemoryPool* pool, int64_t num_rows,
                                                 int64_t num_batches,
                                                 HashFilter* build_target) {
  hardware_flags_ = hardware_flags;
  build_target_ = build_target;
  total_row_nums_ = num_rows;

  constexpr int kMaxLogNumPrtns = 8;
  log_num_prtns_ = std::min(kMaxLogNumPrtns, arrow::bit_util::Log2(num_threads));

  RETURN_NOT_OK(build_target->CreateEmpty(num_rows));

  return arrow::Status::OK();
}

arrow::Status HashFilterBuilder_Parallel::PushNextBatch(size_t thread_id, int64_t num_rows, int32_t* values) {
  PushNextBatchImp(thread_id, num_rows, values);
  return arrow::Status::OK();
}

vector<idx_t> HashFilterBuilder_Parallel::BuiltCols() {
  return build_target_->BoundColsBuilt;
}

void HashFilterBuilder_Parallel::PushNextBatchImp(size_t thread_id, int64_t num_rows,
                                                  int32_t* values) {
    build_target_->Insert(hardware_flags_, num_rows, values);
}

void HashFilterBuilder_Parallel::Print() {
  for(auto itr = build_target_->hash_set.begin(); itr != build_target_->hash_set.end(); itr++) {
    std::cout << *itr << " ";
  }
  std::cout << std::endl;
}

void HashFilterBuilder_Parallel::CleanUp() {
}

void HashFilterBuilder_Parallel::Merge() {
}
}