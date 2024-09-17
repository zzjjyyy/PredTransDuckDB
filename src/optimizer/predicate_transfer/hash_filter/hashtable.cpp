#include "duckdb/optimizer/predicate_transfer/hash_filter/hashtable.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"

#include <iostream>

namespace duckdb {
using ValidityBytes = HashTable::ValidityBytes;
using ScanStructure = HashTable::ScanStructure;

HashTable::HashTable(BufferManager &buffer_manager_p, vector<LogicalType> condition_types)
    : buffer_manager(buffer_manager_p), radix_bits(INITIAL_RADIX_BITS) {
	// Types for the layout
	vector<LogicalType> layout_types(condition_types);
	layout_types.emplace_back(LogicalType::HASH);
	layout.Initialize(layout_types, false);
    for(auto& condition_type : condition_types) {
        predicates.push_back(ExpressionType::COMPARE_EQUAL);
    }
    row_matcher.Initialize(false, layout, predicates);
	row_matcher_no_match_sel.Initialize(true, layout, predicates);

	const auto &offsets = layout.GetOffsets();
	tuple_size = offsets[condition_types.size()];
	pointer_offset = offsets.back();
	entry_size = layout.GetRowWidth();

	data_collection = make_uniq<TupleDataCollection>(buffer_manager, layout);
	sink_collection = make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
}

HashTable::~HashTable() {
}

void HashTable::Merge(HashTable &other) {
	{
		lock_guard<mutex> guard(data_lock);
		data_collection->Combine(*other.data_collection);
	}
	sink_collection->Combine(*other.sink_collection);
}

void HashTable::ApplyBitmask(Vector &hashes, idx_t count) {
	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(!ConstantVector::IsNull(hashes));
		auto indices = ConstantVector::GetData<hash_t>(hashes);
		*indices = *indices & bitmask;
	} else {
		hashes.Flatten(count);
		auto indices = FlatVector::GetData<hash_t>(hashes);
		for (idx_t i = 0; i < count; i++) {
			indices[i] &= bitmask;
		}
	}
}

void HashTable::ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers) {
	UnifiedVectorFormat hdata;
	hashes.ToUnifiedFormat(count, hdata);

	auto hash_data = UnifiedVectorFormat::GetData<hash_t>(hdata);
	auto result_data = FlatVector::GetData<data_ptr_t *>(pointers);
	auto main_ht = reinterpret_cast<data_ptr_t *>(hash_map.get());
	for (idx_t i = 0; i < count; i++) {
		auto rindex = sel.get_index(i);
		auto hindex = hdata.sel->get_index(rindex);
		auto hash = hash_data[hindex];
		result_data[rindex] = main_ht + (hash & bitmask);
	}
}

void HashTable::Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes) {
	if (count == keys.size()) {
		// no null values are filtered: use regular hash functions
		VectorOperations::Hash(keys.data[0], hashes, keys.size());
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], keys.size());
		}
	} else {
		// null values were filtered: use selection vector
		VectorOperations::Hash(keys.data[0], hashes, sel, count);
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], sel, count);
		}
	}
}

static idx_t FilterNullValues(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                              SelectionVector &result) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (vdata.validity.RowIsValid(key_idx)) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}

void HashTable::Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys) {
	if (keys.size() == 0) {
		return;
	}

	// build a chunk to append to the data collection [keys, hash]
	DataChunk source_chunk;
	source_chunk.InitializeEmpty(layout.GetTypes());
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		source_chunk.data[i].Reference(keys.data[i]);
	}
	idx_t col_offset = keys.ColumnCount();
	Vector hash_values(LogicalType::HASH);
	source_chunk.data[col_offset].Reference(hash_values);
	source_chunk.SetCardinality(keys);
	// ToUnifiedFormat the source chunk
	TupleDataCollection::ToUnifiedFormat(append_state.chunk_state, source_chunk);

	// prepare the keys for processing
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, append_state.chunk_state.vector_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Hash(keys, *current_sel, added_count, hash_values);
	// Re-reference and ToUnifiedFormat the hash column after computing it
	source_chunk.data[col_offset].Reference(hash_values);
	hash_values.ToUnifiedFormat(source_chunk.size(), append_state.chunk_state.vector_data.back().unified);
	// We already called TupleDataCollection::ToUnifiedFormat, so we can AppendUnified here
	sink_collection->AppendUnified(append_state, source_chunk, *current_sel, added_count);
}

idx_t HashTable::PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = FlatVector::IncrementalSelectionVector();
	idx_t added_count = keys.size();

	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		auto &col_key_data = vector_data[col_idx].unified;
		if (col_key_data.validity.AllValid()) {
			continue;
		}
		added_count = FilterNullValues(col_key_data, *current_sel, added_count, sel);
		// null values are NOT equal for this column, filter them out
		current_sel = &sel;
	}
	return added_count;
}

template <bool PARALLEL>
static inline void InsertHashesLoop(atomic<data_ptr_t> pointers[], const hash_t indices[], const idx_t count,
                                    const data_ptr_t key_locations[], const idx_t pointer_offset) {
	for (idx_t i = 0; i < count; i++) {
		const auto index = indices[i];
		if (PARALLEL) {
			data_ptr_t head;
			do {
				head = pointers[index];
				Store<data_ptr_t>(head, key_locations[i] + pointer_offset);
			} while (!std::atomic_compare_exchange_weak(&pointers[index], &head, key_locations[i]));
		} else {
			// set prev in current key to the value (NOTE: this will be nullptr if there is none)
			Store<data_ptr_t>(pointers[index], key_locations[i] + pointer_offset);

			// set pointer to current tuple
			pointers[index] = key_locations[i];
		}
	}
}

void HashTable::InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[], bool parallel) {
	D_ASSERT(hashes.GetType().id() == LogicalType::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes, count);

	hashes.Flatten(count);
	D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);

	auto pointers = reinterpret_cast<atomic<data_ptr_t> *>(hash_map.get());
	auto indices = FlatVector::GetData<hash_t>(hashes);

	if (parallel) {
		InsertHashesLoop<true>(pointers, indices, count, key_locations, pointer_offset);
	} else {
		InsertHashesLoop<false>(pointers, indices, count, key_locations, pointer_offset);
	}
}

void HashTable::InitializePointerTable() {
	idx_t capacity = PointerTableCapacity(Count());
	D_ASSERT(IsPowerOfTwo(capacity));

	if (hash_map.get()) {
		// There is already a hash map
		auto current_capacity = hash_map.GetSize() / sizeof(data_ptr_t);
		if (capacity > current_capacity) {
			// Need more space
			hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(data_ptr_t));
		} else {
			// Just use the current hash map
			capacity = current_capacity;
		}
	} else {
		// Allocate a hash map
		hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(data_ptr_t));
	}
	D_ASSERT(hash_map.GetSize() == capacity * sizeof(data_ptr_t));

	// initialize HT with all-zero entries
	std::fill_n(reinterpret_cast<data_ptr_t *>(hash_map.get()), capacity, nullptr);

	bitmask = capacity - 1;
}

void HashTable::Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel) {
	// Pointer table should be allocated
	D_ASSERT(hash_map.get());

	Vector hashes(LogicalType::HASH);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);

	TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, chunk_idx_from,
	                                chunk_idx_to, false);
	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			hash_data[i] = Load<hash_t>(row_locations[i] + pointer_offset);
		}
		InsertHashes(hashes, count, row_locations, parallel);
	} while (iterator.Next());
}

unique_ptr<ScanStructure> HashTable::InitializeScanStructure(DataChunk &keys, TupleDataChunkState &key_state,
                                                             const SelectionVector *&current_sel) {
	D_ASSERT(Count() > 0); // should be handled before

	// set up the scan structure
	auto ss = make_uniq<ScanStructure>(*this, key_state);

	ss->found_match = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
	memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);

	// first prepare the keys for probing
	TupleDataCollection::ToUnifiedFormat(key_state, keys);
	ss->count = PrepareKeys(keys, key_state.vector_data, current_sel, ss->sel_vector, false);
	return ss;
}

unique_ptr<ScanStructure> HashTable::Probe(DataChunk &keys, TupleDataChunkState &key_state,
                                           Vector *precomputed_hashes) {
	const SelectionVector *current_sel;
	auto ss = InitializeScanStructure(keys, key_state, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	if (precomputed_hashes) {
		ApplyBitmask(*precomputed_hashes, *current_sel, ss->count, ss->pointers);
	} else {
		// hash all the keys
		Vector hashes(LogicalType::HASH);
		Hash(keys, *current_sel, ss->count, hashes);

		// now initialize the pointers of the scan structure based on the hashes
		ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);
	}

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);
	return ss;
}

ScanStructure::ScanStructure(HashTable &ht_p, TupleDataChunkState &key_state_p)
    : key_state(key_state_p), pointers(LogicalType::POINTER), sel_vector(STANDARD_VECTOR_SIZE), ht(ht_p),
      finished(false) {
}

void ScanStructure::Next(DataChunk &keys, SelectionVector &match_sel, idx_t &result_count) {
	if (finished) {
		return;
	}
	NextSemiJoin(keys, match_sel, result_count);
}

bool ScanStructure::PointersExhausted() {
	return count == 0;
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel) {
	// Start with the scan selection
	for (idx_t i = 0; i < this->count; ++i) {
		match_sel.set_index(i, this->sel_vector.get_index(i));
	}
	idx_t no_match_count = 0;

	auto &matcher = no_match_sel ? ht.row_matcher_no_match_sel : ht.row_matcher;
	return matcher.Match(keys, key_state.vector_data, match_sel, this->count, ht.layout, pointers, no_match_sel,
	                     no_match_count);
}

void ScanStructure::AdvancePointers(const SelectionVector &sel, idx_t sel_count) {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->pointers);
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx] + ht.pointer_offset);
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void ScanStructure::InitializeSelectionVector(const SelectionVector *&current_sel) {
	idx_t non_empty_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
	auto cnt = count;
	for (idx_t i = 0; i < cnt; i++) {
		const auto idx = current_sel->get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx]);
		if (ptrs[idx]) {
			sel_vector.set_index(non_empty_count++, idx);
		}
	}
	count = non_empty_count;
}

void ScanStructure::AdvancePointers() {
	AdvancePointers(this->sel_vector, this->count);
}

void ScanStructure::ScanKeyMatches(DataChunk &keys, SelectionVector &sel, idx_t &result_count) {
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			found_match[match_sel.get_index(i)] = true;
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
	for (idx_t i = 0; i < keys.size(); i++) {
		sel.set_index(result_count, i);
		result_count += found_match[i];
	}
}

void ScanStructure::NextSemiJoin(DataChunk &keys, SelectionVector &sel, idx_t &result_count) {
	// first scan for key matches
	ScanKeyMatches(keys, sel, result_count);
	finished = true;
}

void HashTable::Unpartition() {
	data_collection = sink_collection->GetUnpartitioned();
}
}