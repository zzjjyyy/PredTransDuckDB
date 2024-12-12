#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"

#include <sys/types.h>
#include <thread>

namespace duckdb {
#ifdef UseHashFilter
PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, vector<shared_ptr<HashFilter>> bf, idx_t estimated_cardinality)
#else
PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, vector<shared_ptr<BlockedBloomFilter>> bf, idx_t estimated_cardinality)
#endif
    : PhysicalOperator(PhysicalOperatorType::CREATE_BF, std::move(types), estimated_cardinality), bf_to_create(bf) {
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op)
		: op(op), temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)),
		  partition_start(0), partition_end(0), max_partition_count(0),
		  max_partition_size(0) {
			total_partitioned_data = make_uniq<RadixPartitionedColumnData>(context, op.types, 3, op.types.size() - 1);
			total_data = make_uniq<ColumnDataCollection>(context, op.types);
		}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);

	mutex glock;
	const PhysicalCreateBF &op;

	unique_ptr<RadixPartitionedColumnData> total_partitioned_data;

	unique_ptr<ColumnDataCollection> total_data;
#ifdef UseHashFilter
	vector<shared_ptr<HashFilterBuilder>> builders;
#else
	vector<shared_ptr<BloomFilterBuilder>> builders;
#endif
	vector<unique_ptr<PartitionedColumnData>> local_data_collections;

	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	bool external;
	idx_t max_partition_size;
	idx_t max_partition_count;

	idx_t partition_start;
	idx_t partition_end;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op) {
		local_data = make_uniq<RadixPartitionedColumnData>(context, op.types, 3, op.types.size() - 1);
		local_data->Initialize();
		local_data->InitializeAppendState(append_state);
	}

	PartitionedColumnDataAppendState append_state;
	unique_ptr<RadixPartitionedColumnData> local_data;
};

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();
	state.local_data->Append(state.append_state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context,
                                         		OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();
	lock_guard<mutex> lock(gstate.glock);
	gstate.local_data_collections.emplace_back(std::move(state.local_data));
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<std::thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<std::thread> internal_thread;
#endif
};

class CreateBFFinalizeTask : public ExecutorTask {
public:
	CreateBFFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, CreateBFGlobalSinkState &sink_p,
	                     idx_t chunk_idx_from_p, idx_t chunk_idx_to_p, size_t num_threads)
	    : ExecutorTask(context), event(std::move(event_p)), sink(sink_p), chunk_idx_from(chunk_idx_from_p),
	      chunk_idx_to(chunk_idx_to_p),
		  threads(TaskScheduler::GetScheduler(context).threads) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ThreadContext tcontext(this->executor.context);
		tcontext.profiler.StartOperator(&sink.op);
		size_t thread_id = 0;
		std::thread::id threadId = std::this_thread::get_id();
		for(size_t i = 0; i < threads.size(); i++) {
			if (threadId == threads[i]->internal_thread->get_id()) {
				thread_id = i + 1;
				break;
			}
		}
		for(idx_t i = chunk_idx_from; i < chunk_idx_to; i++) {
			DataChunk chunk;
			sink.total_data->InitializeScanChunk(chunk);
			sink.total_data->FetchChunk(i, chunk);
			for(auto &builder : sink.builders) {
				auto cols = builder->BuiltCols();
#ifdef UseHashFilter
				DataChunk input;
				input.SetCardinality(chunk.size());
				for(int i = 0; i < cols.size(); i++) {
				   	Vector v = chunk.data[cols[i]];
					input.data.emplace_back(v);
				}
				builder->PushNextBatch(thread_id, chunk.size(), input);
#else
				Vector hashes(LogicalType::HASH);
				VectorOperations::Hash(chunk.data[cols[0]], hashes, chunk.size());
				for(int i = 1; i < cols.size(); i++) {
					VectorOperations::CombineHash(hashes, chunk.data[cols[i]], chunk.size());
				}
				if(hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
					hashes.Flatten(chunk.size());
				}
				builder->PushNextBatch(thread_id, chunk.size(), (hash_t*)hashes.GetData());
#endif
			}
		}
#ifdef UseHashFilter
		for (auto &builder : sink.builders) {
			builder->build_target_->hash_table->Unpartition();
			builder->build_target_->hash_table->InitializePointerTable();
			const auto chunk_count = builder->build_target_->hash_table->GetDataCollection().ChunkCount();
			if(chunk_count > 0) {
				builder->build_target_->hash_table->Finalize(0, chunk_count, false);
			}
		}
#endif
		if (sink.external && sink.op.LoadInMem(RadixPartitioning::NumberOfPartitions(3), sink.max_partition_size, sink)) {
			vector<shared_ptr<Task>> finalize_tasks;
			auto &buffer = sink.total_data;
			const auto chunk_count = buffer->ChunkCount();
			finalize_tasks.push_back(make_uniq<CreateBFFinalizeTask>(event, this->executor.context, sink, 0, chunk_count, 1));
			event->SetTasks(std::move(finalize_tasks));
		} else {
			/* We stop load and wait for source for load */
			sink.partition_start = 0;
			sink.partition_end = 0;
		}
		event->FinishTask();
		tcontext.profiler.EndOperator(nullptr);
		this->executor.Flush(tcontext);
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	CreateBFGlobalSinkState &sink;
	idx_t chunk_idx_from;
	idx_t chunk_idx_to;
	vector<duckdb::unique_ptr<duckdb::SchedulerThread>> &threads;
};

class CreateBFFinalizeEvent : public BasePipelineEvent {
	public:
	CreateBFFinalizeEvent(Pipeline &pipeline_p, CreateBFGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	CreateBFGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> finalize_tasks;
		auto &buffer = sink.total_data;
		const auto chunk_count = buffer->ChunkCount();
		const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		if (num_threads == 1 || (buffer->Count() < PARALLEL_CONSTRUCT_THRESHOLD && !context.config.verify_parallelism)) {
			// Single-threaded finalize
			finalize_tasks.push_back(make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink, 0, chunk_count, 1));
		} else {
			// Parallel finalize
			auto chunks_per_thread = MaxValue<idx_t>((chunk_count + num_threads - 1) / num_threads, 1);

			idx_t chunk_idx = 0;
			for (idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
				auto chunk_idx_from = chunk_idx;
				auto chunk_idx_to = MinValue<idx_t>(chunk_idx_from + chunks_per_thread, chunk_count);
				finalize_tasks.push_back(make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink,
				                                                         chunk_idx_from, chunk_idx_to, num_threads));
				chunk_idx = chunk_idx_to;
				if (chunk_idx == chunk_count) {
					break;
				}
			}
		}
		SetTasks(std::move(finalize_tasks));
	}

	// void FinishEvent() override {
	// }

	static constexpr const idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
};

void CreateBFGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	auto new_event = make_shared<CreateBFFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(new_event));
}

bool PhysicalCreateBF::LoadInMem(idx_t num_partitions, idx_t max_partition_size, CreateBFGlobalSinkState &sink) const {
	sink.total_data->Reset();

	const auto max_partition_data_size = max_partition_size;
	if (sink.partition_end == num_partitions) {
		return false;
	}
	sink.temporary_memory_state->SetMinimumReservation(max_partition_data_size);
	// Start where we left off
	auto &partitions = sink.total_partitioned_data->GetPartitions();
	sink.partition_start = sink.partition_end;

	// Determine how many partitions we can do next (at least one)
	idx_t count = 0;
	idx_t data_size = 0;
	idx_t partition_idx;
	for (partition_idx = sink.partition_start; partition_idx < num_partitions; partition_idx++) {
		auto incl_count = count + partitions[partition_idx]->Count();
		auto incl_data_size = data_size + partitions[partition_idx]->SizeInBytes();
		auto incl_ht_size = incl_data_size;
		if (count > 0 && incl_ht_size > max_partition_data_size) {
			break;
		}
		count = incl_count;
		data_size = incl_data_size;
	}
	sink.partition_end = partition_idx;
	// Move the partitions to the main data collection
	for (partition_idx = sink.partition_start; partition_idx < sink.partition_end; partition_idx++) {
		sink.total_data->Combine(*partitions[partition_idx]);
	}
	return true;
}

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  		OperatorSinkFinalizeInput &input) const {
	ThreadContext tcontext(context);
	tcontext.profiler.StartOperator(this);
	auto &sink = input.global_state.Cast<CreateBFGlobalSinkState>();

	const auto num_partitions = RadixPartitioning::NumberOfPartitions(3);
	vector<idx_t> partition_sizes(num_partitions, 0);
	vector<idx_t> partition_counts(num_partitions, 0);
	for (auto &sink_collection : sink.local_data_collections) {
		D_ASSERT(partition_sizes.size() == sink_collection->GetPartitions().size());
		D_ASSERT(partition_sizes.size() == partition_counts.size());
		for (idx_t i = 0; i < sink_collection->GetPartitions().size(); i++) {
			auto &partition = *sink_collection->GetPartitions()[i];
			partition_sizes[i] += partition.SizeInBytes();
			partition_counts[i] += partition.Count();
		}
	}

	idx_t total_size = 0;
	idx_t total_count = 0;
	sink.max_partition_size = 0;
	sink.max_partition_count = 0;
	for (idx_t i = 0; i < num_partitions; i++) {
		total_size += partition_sizes[i];
		total_count += partition_counts[i];
		auto partition_size = partition_sizes[i];
		if (partition_size > sink.max_partition_size) {
			sink.max_partition_size = partition_sizes[i];
			sink.max_partition_count = partition_counts[i];
		}
	}

	if (total_count == 0) {
		total_size = 0;
	}

	sink.temporary_memory_state->SetRemainingSize(context, total_size);

	sink.external = sink.temporary_memory_state->GetReservation() < total_size;
	for(auto& local_data : sink.local_data_collections) {
		sink.total_partitioned_data->Combine(*local_data);
	}
	sink.local_data_collections.clear();
	int64_t num_rows = 0;
	if (sink.external) {
		std::cout << "CreateBF external" << std::endl;
		num_rows = total_count;
		LoadInMem(num_partitions, sink.max_partition_size, sink);
	} else {
		sink.total_data = sink.total_partitioned_data->GetUnpartitioned();
		num_rows = sink.total_data->Count();
	}

	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	for (auto &filter : bf_to_create) {
		if (num_threads == 1) {
#ifdef UseHashFilter
			auto builder = make_shared<HashFilterBuilder_SingleThreaded>();
			auto cols = filter->BoundColsBuilt;
			vector<LogicalType> layouts;
			for(int i = 0; i < cols.size(); i++) {
				layouts.emplace_back(sink.total_data.Types()[cols[i]]);
			}
			builder->Begin(1, arrow::internal::CpuInfo::AVX2, &BufferManager::GetBufferManager(context), layouts, 0, filter.get());
#else
			auto builder = make_shared<BloomFilterBuilder_SingleThreaded>();
			builder->Begin(1, arrow::internal::CpuInfo::AVX2, arrow::default_memory_pool(), num_rows, 0, filter.get());
#endif
			sink.builders.emplace_back(builder);
		} else {
#ifdef UseHashFilter
			auto builder = make_shared<HashFilterBuilder_Parallel>();
			auto cols = filter->BoundColsBuilt;
			vector<LogicalType> layouts;
			for(int i = 0; i < cols.size(); i++) {
				layouts.emplace_back(sink.total_data.Types()[cols[i]]);
			}
			builder->Begin(num_threads, arrow::internal::CpuInfo::AVX2, &BufferManager::GetBufferManager(context), layouts, 0, filter.get());
#else
			auto builder = make_shared<BloomFilterBuilder_Parallel>();
			builder->Begin(num_threads, arrow::internal::CpuInfo::AVX2, arrow::default_memory_pool(), num_rows, 0, filter.get());
#endif
			sink.builders.emplace_back(builder);
		}
	}

	sink.ScheduleFinalize(pipeline, event);
	tcontext.profiler.EndOperator(nullptr);
	context.GetExecutor().Flush(tcontext);
	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateBFGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateBFLocalSinkState>(context.client, *this);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CreateBFGlobalSourceState : public GlobalSourceState {
public:
	CreateBFGlobalSourceState(ClientContext &context, const PhysicalCreateBF &op)
		: context(context) {
		D_ASSERT(op.sink_state);
		auto &gstate = op.sink_state->Cast<CreateBFGlobalSinkState>();
		gstate.total_data->InitializeScan(scan_state);
		partition_id = 0;
	}

	ColumnDataParallelScanState scan_state;
	ClientContext &context;
	vector<pair<idx_t, idx_t>> chunks_todo;
	std::atomic<idx_t> partition_id;

	idx_t MaxThreads() override {
		return TaskScheduler::GetScheduler(context).NumberOfThreads();
	}
};

class CreateBFLocalSourceState : public LocalSourceState {
public:
	CreateBFLocalSourceState() {
		local_current_chunk_id = 0;
		initial = true;
	}
	ColumnDataLocalScanState scan_state;

	idx_t local_current_chunk_id;
	idx_t local_partition_id;
	idx_t chunk_from;
	idx_t chunk_to;
	bool initial;
};

unique_ptr<GlobalSourceState> PhysicalCreateBF::GetGlobalSourceState(ClientContext &context) const {
	auto state = make_uniq<CreateBFGlobalSourceState>(context, *this);
	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	if (gstate.external) {
		LoadInMem(RadixPartitioning::NumberOfPartitions(3), gstate.max_partition_size, gstate);
	}
	auto chunk_count = gstate.total_data->ChunkCount();
	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	auto chunks_per_thread = MaxValue<idx_t>((chunk_count + num_threads - 1) / num_threads, 1);
	idx_t chunk_idx = 0;
	for(idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
		if (chunk_idx == chunk_count) {
			break;
		}
		auto chunk_idx_from = chunk_idx;
		auto chunk_idx_to = MinValue<idx_t>(chunk_idx_from + chunks_per_thread, chunk_count);
		state->chunks_todo.emplace_back(chunk_idx_from, chunk_idx_to);
		chunk_idx = chunk_idx_to;
	}
	return unique_ptr_cast<CreateBFGlobalSourceState, GlobalSourceState>(std::move(state));
}

unique_ptr<LocalSourceState> PhysicalCreateBF::GetLocalSourceState(ExecutionContext &context,
	                                                 			   GlobalSourceState &gstate) const {
	return make_uniq<CreateBFLocalSourceState>();
}

SourceResultType PhysicalCreateBF::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateBFLocalSourceState>();
	auto &state = input.global_state.Cast<CreateBFGlobalSourceState>();
	if(lstate.initial) {
		lstate.local_partition_id = state.partition_id++;
		lstate.initial = false;
		if (lstate.local_partition_id >= state.chunks_todo.size()) {
			return SourceResultType::FINISHED;
		}
		lstate.chunk_from = state.chunks_todo[lstate.local_partition_id].first;
		lstate.chunk_to = state.chunks_todo[lstate.local_partition_id].second;
	}
	if (lstate.local_current_chunk_id == 0) {
		lstate.local_current_chunk_id = lstate.chunk_from;
	} else if(lstate.local_current_chunk_id >= lstate.chunk_to) {
		if (gstate.external && LoadInMem(RadixPartitioning::NumberOfPartitions(3), gstate.max_partition_size, gstate)) {
			auto chunk_count = gstate.total_data->ChunkCount();
			const idx_t num_threads = 1;
			auto chunks_per_thread = MaxValue<idx_t>((chunk_count + num_threads - 1) / num_threads, 1);
			auto chunk_idx_from = 0;
			auto chunk_idx_to = chunk_count;
			state.chunks_todo.clear();
			state.chunks_todo.emplace_back(chunk_idx_from, chunk_idx_to);
			lstate.local_current_chunk_id = 0;
			lstate.initial = true;
			return SourceResultType::HAVE_MORE_OUTPUT;
		} else {
			return SourceResultType::FINISHED;
		}
	}
	gstate.total_data->FetchChunk(lstate.local_current_chunk_id++, chunk);
	return SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalCreateBF::ParamsToString() const {
	string result;
	return result;
}

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	// operator is a sink, build a pipeline
	D_ASSERT(children.size() == 1);

	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(*children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}

/* Add related createBF dependency */
void PhysicalCreateBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);
	if (this_pipeline == nullptr) {
		// we create a new pipeline starting from the child
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(*children[0]);
	} else {
		current.AddDependency(this_pipeline);
	}
}
}