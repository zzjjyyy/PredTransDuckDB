#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"
#include "duckdb/optimizer/predicate_transfer/setting.hpp"

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
	// std::random_device rd;
	// auto seed = rd();
	// random_engine.seed(seed);
}

double CostModel::ComputeCost(JoinNode &left, JoinNode &right) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
#ifdef ExactLeftDeep
	return join_cost + left.cost + 1.2 * right.cost;
#else
	return join_cost + left.cost + right.cost;
#endif
}

} // namespace duckdb
