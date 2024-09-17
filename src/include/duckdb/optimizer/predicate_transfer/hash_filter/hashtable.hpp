#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/row_operations/row_matcher.hpp"

namespace duckdb {
class BufferManager;
class BufferHandle;
class ColumnDataCollection;
class PartitionedTupleData;
struct TupleDataChunkState;
struct PartitionedTupleDataAppendState;
struct ColumnDataAppendState;
struct ClientConfig;

class HashTable {
public:
	using ValidityBytes = TemplatedValidityMask<uint8_t>;

	struct ScanStructure {
		TupleDataChunkState &key_state;
		Vector pointers;
		idx_t count;
		SelectionVector sel_vector;
		// whether or not the given tuple has found a match
		unsafe_unique_array<bool> found_match;
		HashTable &ht;
		bool finished;

		explicit ScanStructure(HashTable &ht, TupleDataChunkState &key_state);
		//! Get the next batch of data from the scan structure
		void Next(DataChunk &keys, SelectionVector &match_sel, idx_t &result_count);
		//! Are pointer chains all pointing to NULL?
		bool PointersExhausted();

	private:
		//! Next operator for the semi join
		void NextSemiJoin(DataChunk &keys, SelectionVector &match_sel, idx_t &result_count);
		
		//! Scan the hashtable for matches of the specified keys, setting the found_match[] array to true or false
		//! for every tuple
		void ScanKeyMatches(DataChunk &keys, SelectionVector &match_sel, idx_t &result_count);

	public:
		void InitializeSelectionVector(const SelectionVector *&current_sel);
		void AdvancePointers();
		void AdvancePointers(const SelectionVector &sel, idx_t sel_count);
		idx_t ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel);
	};

public:
    HashTable(BufferManager &buffer_manager, vector<LogicalType> condition_types);
    ~HashTable();

    //! Add the given data to the HT
	void Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys);
    //! Merge another HT into this one
	void Merge(HashTable &other);
    //! Combines the partitions in sink_collection into data_collection, as if it were not partitioned
	void Unpartition();
	//! Initialize the pointer table for the probe
	void InitializePointerTable();
	//! Finalize the build of the HT, constructing the actual hash table and making the HT ready for probing.
	//! Finalize must be called before any call to Probe, and after Finalize is called Build should no longer be
	//! ever called.
	void Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel);
	//! Probe the HT with the given input chunk, resulting in the given result
	unique_ptr<ScanStructure> Probe(DataChunk &keys, TupleDataChunkState &key_state,
	                                Vector *precomputed_hashes = nullptr);

    idx_t Count() const {
		return data_collection->Count();
	}
	idx_t SizeInBytes() const {
		return data_collection->SizeInBytes();
	}

	PartitionedTupleData &GetSinkCollection() {
		return *sink_collection;
	}

	TupleDataCollection &GetDataCollection() {
		return *data_collection;
	}

	//! BufferManager
	BufferManager &buffer_manager;
	//! The types of the keys used in equality comparison
	vector<LogicalType> equality_types;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! The comparison predicates
	vector<ExpressionType> predicates;
	//! Data column layout
	TupleDataLayout layout;
	//! Efficiently matches rows
	RowMatcher row_matcher;
	RowMatcher row_matcher_no_match_sel;
	//! The size of an entry as stored in the HashTable
	idx_t entry_size;
	//! The total tuple size
	idx_t tuple_size;
	//! Next pointer offset in tuple
	idx_t pointer_offset;
	//! Whether or not any of the key elements contain NULL
	bool has_null;
	//! Bitmask for getting relevant bits from the hashes to determine the position
	uint64_t bitmask;
    
private:
	unique_ptr<ScanStructure> InitializeScanStructure(DataChunk &keys, TupleDataChunkState &key_state,
	                                                  const SelectionVector *&current_sel);
	void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	//! Apply a bitmask to the hashes
	void ApplyBitmask(Vector &hashes, idx_t count);
	void ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers);

private:
	//! Insert the given set of locations into the HT with the given set of hashes
	void InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[], bool parallel);

	idx_t PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data, const SelectionVector *&current_sel,
	                  SelectionVector &sel, bool build_side);

	//! Lock for combining data_collection when merging HTs
	mutex data_lock;
	//! Partitioned data collection that the data is sunk into when building
	unique_ptr<PartitionedTupleData> sink_collection;
	//! The DataCollection holding the main data of the hash table
	unique_ptr<TupleDataCollection> data_collection;
	//! The hash map of the HT, created after finalization
	AllocatedData hash_map;

	//! Copying not allowed
	HashTable(const HashTable &) = delete;

public:
	static constexpr const idx_t INITIAL_RADIX_BITS = 3;

	static idx_t PointerTableCapacity(idx_t count) {
		return MaxValue<idx_t>(NextPowerOfTwo(count * 2), 1 << 10);
	}
	//! Size of the pointer table (in bytes)
	static idx_t PointerTableSize(idx_t count) {
		return PointerTableCapacity(count) * sizeof(data_ptr_t);
	}

private:
	idx_t radix_bits;
};
}