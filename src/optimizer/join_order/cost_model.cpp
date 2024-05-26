#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/cost_model.hpp"

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
	// std::uniform_real_distribution<double> dist(-1, 1);
	// double exp = dist(random_engine);
	// auto join_cost = join_card * std::pow(10, exp);
	auto join_cost = join_card;
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb
