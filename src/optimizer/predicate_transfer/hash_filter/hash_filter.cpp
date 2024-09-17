#include "duckdb/optimizer/predicate_transfer/hash_filter/hash_filter.hpp"
#include <random>
#include "arrow/acero/util.h"       // PREFETCH
#include "arrow/util/bit_util.h"    // Log2
#include "arrow/util/bitmap_ops.h"  // CountSetBits
#include "arrow/util/config.h"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include <iostream>

namespace duckdb {
arrow::Status HashFilter::CreateEmpty(BufferManager* buffer, vector<LogicalType> layouts) {
  hash_table = make_shared<HashTable>(*buffer, layouts);
  return arrow::Status::OK();
}

void HashFilter::Find(int64_t hardware_flags, int64_t num_rows, DataChunk& values,
                      SelectionVector &sel, idx_t &result_count, bool enable_prefetch) const {
  TupleDataChunkState key_state;
  TupleDataCollection::InitializeChunkState(key_state, values.GetTypes());
  auto ss = hash_table->Probe(values, key_state);
  ss->Next(values, sel, result_count);
}

arrow::Status HashFilterBuilder_SingleThreaded::Begin(size_t /*num_threads*/,
                                                      int64_t hardware_flags, BufferManager* buffer,
                                                      vector<LogicalType> layouts, int64_t /*num_batches*/,
                                                      HashFilter* build_target) {
  hardware_flags_ = hardware_flags;
  build_target_ = build_target;

  RETURN_NOT_OK(build_target->CreateEmpty(buffer, layouts));

  return arrow::Status::OK();
}

arrow::Status HashFilterBuilder_SingleThreaded::PushNextBatch(size_t /*thread_index*/,
                                                              int64_t num_rows,
                                                              DataChunk& values) {
  auto local_hash_table = std::make_shared<HashTable>(build_target_->hash_table->buffer_manager, values.GetTypes());
  PartitionedTupleDataAppendState append_state;
  local_hash_table->GetSinkCollection().InitializeAppendState(append_state);
  local_hash_table->Build(append_state, values);
  build_target_->hash_table->Merge(*local_hash_table);
  return arrow::Status::OK();
}

vector<idx_t> HashFilterBuilder_SingleThreaded::BuiltCols() {
  return build_target_->BoundColsBuilt;
}

void HashFilterBuilder_SingleThreaded::Print() {
  std::cout << std::endl;
}

arrow::Status HashFilterBuilder_Parallel::Begin(size_t num_threads, int64_t hardware_flags,
                                                BufferManager* buffer, vector<LogicalType> layouts,
                                                int64_t num_batches,
                                                HashFilter* build_target) {
  hardware_flags_ = hardware_flags;
  build_target_ = build_target;

  constexpr int kMaxLogNumPrtns = 8;
  log_num_prtns_ = std::min(kMaxLogNumPrtns, arrow::bit_util::Log2(num_threads));

  RETURN_NOT_OK(build_target->CreateEmpty(buffer, layouts));

  return arrow::Status::OK();
}

arrow::Status HashFilterBuilder_Parallel::PushNextBatch(size_t thread_id, int64_t num_rows, DataChunk& values) {
  auto local_hash_table = std::make_shared<HashTable>(build_target_->hash_table->buffer_manager, values.GetTypes());
  PartitionedTupleDataAppendState append_state;
  local_hash_table->GetSinkCollection().InitializeAppendState(append_state);
  local_hash_table->Build(append_state, values);
  build_target_->hash_table->Merge(*local_hash_table);
  return arrow::Status::OK();
}

vector<idx_t> HashFilterBuilder_Parallel::BuiltCols() {
  return build_target_->BoundColsBuilt;
}

void HashFilterBuilder_Parallel::Print() {
  std::cout << std::endl;
}

void HashFilterBuilder_Parallel::CleanUp() {
}

void HashFilterBuilder_Parallel::Merge() {
}
}