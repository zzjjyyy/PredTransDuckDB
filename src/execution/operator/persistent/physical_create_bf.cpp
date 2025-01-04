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
#include "duckdb/optimizer/predicate_transfer/setting.hpp"

#include <sys/types.h>
#include <thread>

namespace duckdb {
#ifdef UseHashFilter
PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, vector<shared_ptr<HashFilter>> bf, idx_t estimated_cardinality)
#else
PhysicalCreateBF::PhysicalCreateBF(vector<LogicalType> types, vector<shared_ptr<BlockedBloomFilter>> bf, idx_t estimated_cardinality)
#endif
    : PhysicalOperator(PhysicalOperatorType::CREATE_BF, std::move(types), estimated_cardinality), bf_to_create(bf) {
		count_for_debug = make_shared<idx_t>(0);
	}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op)
		: op(op), temporary_memory_state(TemporaryMemoryManager::Get(context).Register(context)),
		  partition_start(0), partition_end(0), max_partition_count(0),
		  max_partition_size(0) {
#ifdef External
			TupleDataLayout layout;
			layout.Initialize(op.types, false);
			total_data = make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(context), layout);
#else
			total_data = make_uniq<ColumnDataCollection>(context, op.types);
#endif
		}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);

	mutex glock;
	const PhysicalCreateBF &op;

	
#ifdef UseHashFilter
	vector<shared_ptr<HashFilterBuilder>> builders;
#else
	vector<shared_ptr<BloomFilterBuilder>> builders;
#endif

#ifdef External
	vector<unique_ptr<TupleDataCollection>> local_data_collections;
	unique_ptr<TupleDataCollection> total_data;
#else
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;
	unique_ptr<ColumnDataCollection> total_data;
#endif
	unique_ptr<TemporaryMemoryState> temporary_memory_state;

	bool external;
	idx_t max_partition_size;
	idx_t max_partition_count;

	idx_t partition_start;
	idx_t partition_end;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op) 
		: client_context(context) {
#ifdef External
		TupleDataLayout layout;
		layout.Initialize(op.types, false);
		auto local_data_partition = make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(context), layout);
		local_data.emplace_back(std::move(local_data_partition));
		local_partition_id = 0;
		temporary_memory_state = TemporaryMemoryManager::Get(context).Register(context);
#else
		local_data = make_uniq<ColumnDataCollection>(context, op.types);
#endif
	}

	ClientContext &client_context;

#ifdef External
	vector<unique_ptr<TupleDataCollection>> local_data;
	idx_t local_partition_id;
	unique_ptr<TemporaryMemoryState> temporary_memory_state;
#else
	unique_ptr<ColumnDataCollection> local_data;
#endif
};

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();
#ifdef External
	if (state.local_data[state.local_partition_id]->SizeInBytes() + 8 * chunk.size() * chunk.ColumnCount() > state.temporary_memory_state->GetReservation()) {
		TupleDataLayout layout;
		layout.Initialize(this->types, false);
		auto local_data_partition = make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(state.client_context), layout);
		state.local_data.emplace_back(std::move(local_data_partition));
		state.local_data[++state.local_partition_id]->Append(chunk);
	} else {
		state.local_data[state.local_partition_id]->Append(chunk);
	}
#else
	state.local_data->Append(chunk);
#endif
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context,
                                         		OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	auto &state = input.local_state.Cast<CreateBFLocalSinkState>();
#ifdef External
	gstate.local_data_collections = std::move(state.local_data);
#else
	lock_guard<mutex> lock(gstate.glock);
	gstate.local_data_collections.emplace_back(std::move(state.local_data));
#endif
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
	      chunk_idx_to(chunk_idx_to_p), threads(TaskScheduler::GetScheduler(context).threads) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ThreadContext tcontext(this->executor.context);
		tcontext.profiler.StartOperator(&sink.op);
#ifdef External
		if (sink.external) {
			for(int i = 0; i < sink.local_data_collections.size(); i++) {
				DataChunk chunk;
				sink.local_data_collections[i]->InitializeChunk(chunk);
				TupleDataScanState state;
				sink.local_data_collections[i]->InitializeScan(state);
				while (true) {
					sink.local_data_collections[i]->Scan(state, chunk);
					if (chunk.size() == 0) {
						break;
					}
					for(auto &builder : sink.builders) {
						auto cols = builder->BuiltCols();
						Vector hashes(LogicalType::HASH);
						VectorOperations::Hash(chunk.data[cols[0]], hashes, chunk.size());
						for(int i = 1; i < cols.size(); i++) {
							VectorOperations::CombineHash(hashes, chunk.data[cols[i]], chunk.size());
						}
						if(hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
							hashes.Flatten(chunk.size());
						}
						builder->PushNextBatch(0, chunk.size(), (hash_t*)hashes.GetData());
					}
				}
			}
		} else {
			DataChunk chunk;
			TupleDataScanState state;
			sink.total_data->InitializeChunk(chunk);
			sink.total_data->InitializeScan(state);
			while (true) {
				sink.total_data->Scan(state, chunk);
				if (chunk.size() == 0) {
					break;
				}
				for(auto &builder : sink.builders) {
					auto cols = builder->BuiltCols();
					Vector hashes(LogicalType::HASH);
					VectorOperations::Hash(chunk.data[cols[0]], hashes, chunk.size());
					for(int i = 1; i < cols.size(); i++) {
						VectorOperations::CombineHash(hashes, chunk.data[cols[i]], chunk.size());
					}
					if(hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
						hashes.Flatten(chunk.size());
					}
					builder->PushNextBatch(0, chunk.size(), (hash_t*)hashes.GetData());
				}
			}
		}
#else
		size_t thread_id = 0;
		std::thread::id threadId = std::this_thread::get_id();
		for(size_t i = 0; i < threads.size(); i++) {
			if (threadId == threads[i]->internal_thread->get_id()) {
				thread_id = i + 1;
				break;
			}
		}
		for (idx_t i = chunk_idx_from; i < chunk_idx_to; i++) {
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
#endif
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

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  		OperatorSinkFinalizeInput &input) const {
	ThreadContext tcontext(context);
	tcontext.profiler.StartOperator(this);
	auto &sink = input.global_state.Cast<CreateBFGlobalSinkState>();
	int64_t num_rows = 0;
	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();

#ifdef External
	auto num_partitions = sink.local_data_collections.size();
	vector<idx_t> partition_sizes(num_partitions, 0);
	vector<idx_t> partition_counts(num_partitions, 0);
	for (int i = 0; i < sink.local_data_collections.size(); i++) {
		D_ASSERT(partition_sizes.size() == sink.local_data_collections.size());
		D_ASSERT(partition_sizes.size() == partition_counts.size());
		partition_sizes[i] += sink.local_data_collections[i]->SizeInBytes();
		partition_counts[i] += sink.local_data_collections[i]->Count();
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
	if (sink.external) {
		std::cout << "External Create BF" << std::endl;
		if (num_threads > 1) {
			throw InternalException("External CreateBF can only be used under single-thread.");
		}
		num_rows = total_count;
	} else {
		for(auto& local_data : sink.local_data_collections) {
			sink.total_data->Combine(*local_data);
		}
		sink.local_data_collections.clear();
		num_rows = sink.total_data->Count();
	}
#else
	for(auto& local_data : sink.local_data_collections) {
		sink.total_data->Combine(*local_data);
	}
	sink.local_data_collections.clear();
	num_rows = sink.total_data->Count();
#endif

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
#ifdef External
		gstate.total_data->InitializeScan(scan_state);
#else
		gstate.total_data->InitializeScan(scan_state);
#endif
		partition_id = 0;
	}

#ifdef External
	TupleDataScanState scan_state;
#else
	ColumnDataParallelScanState scan_state;
#endif
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
#ifdef External
	if(!gstate.external) {
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
	}
#else
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
#endif
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
#ifdef External
	if (gstate.external) {
		if(lstate.initial) {
			lstate.local_partition_id = 0;
			lstate.initial = false;
			gstate.local_data_collections[lstate.local_partition_id]->InitializeScan(state.scan_state);
		}
		gstate.local_data_collections[lstate.local_partition_id]->Scan(state.scan_state, chunk);
		if (chunk.size() == 0) {
			lstate.local_partition_id++;
			if (lstate.local_partition_id >= gstate.local_data_collections.size()) {
				return SourceResultType::FINISHED;
			}
			gstate.local_data_collections[lstate.local_partition_id]->InitializeScan(state.scan_state);
		}
	} else {
		if (lstate.initial) {
			lstate.initial = false;
			gstate.total_data->InitializeScan(state.scan_state);
		}
		gstate.total_data->Scan(state.scan_state, chunk);
		if(chunk.size() == 0) {
			return SourceResultType::FINISHED;
		}
	}
#else
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
		return SourceResultType::FINISHED;
	}
	gstate.total_data->FetchChunk(lstate.local_current_chunk_id++, chunk);
#endif
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