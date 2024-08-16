#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {
LogicalCreateBF::LogicalCreateBF(vector<shared_ptr<BlockedBloomFilter>> bf)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_BF), bf_to_create(bf) {};

void LogicalCreateBF::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
    throw InternalException("Shouldn't go here: LogicalCreateBF::Serialize");
}

unique_ptr<LogicalOperator> LogicalCreateBF::Deserialize(Deserializer &deserializer) {
    throw InternalException("Shouldn't go here: LogicalCreateBF::Deserialize");
	return nullptr;
}

idx_t LogicalCreateBF::EstimateCardinality(ClientContext &context) {
	return children[0]->EstimateCardinality(context);
}

void LogicalCreateBF::ResolveTypes() {
	types = children[0]->types;
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}
}