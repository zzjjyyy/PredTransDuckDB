#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression) : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {
	expressions.push_back(std::move(expression));
	SplitPredicates(expressions);
}

LogicalFilter::LogicalFilter() : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {
}

void LogicalFilter::ResolveTypes() {
	types = MapTypes(children[0]->types, projection_map);
}

unique_ptr<LogicalOperator> LogicalFilter::FastCopy() {
	/* LogicalFilter fields */
	unique_ptr<LogicalFilter> result = make_uniq<LogicalFilter>();
	
	result->types = this->types;
	result->estimated_cardinality = this->estimated_cardinality;
	for (auto &child : this->expressions) {
		result->expressions.push_back(child->Copy());
	}
	result->has_estimated_cardinality = this->has_estimated_cardinality;
	for (auto &child : children) {
		D_ASSERT(child->type == LogicalOperatorType::LOGICAL_GET);
		LogicalGet &get = child->Cast<LogicalGet>();
		result->children.emplace_back(get.FastCopy());
	}
	return unique_ptr_cast<LogicalFilter, LogicalOperator>(std::move(result));
}

vector<ColumnBinding> LogicalFilter::GetColumnBindings() {
	return MapBindings(children[0]->GetColumnBindings(), projection_map);
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates(vector<unique_ptr<Expression>> &expressions) {
	bool found_conjunction = false;
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::CONJUNCTION_AND) {
			auto &conjunction = expressions[i]->Cast<BoundConjunctionExpression>();
			found_conjunction = true;
			// AND expression, append the other children
			for (idx_t k = 1; k < conjunction.children.size(); k++) {
				expressions.push_back(std::move(conjunction.children[k]));
			}
			// replace this expression with the first child of the conjunction
			expressions[i] = std::move(conjunction.children[0]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}

} // namespace duckdb
