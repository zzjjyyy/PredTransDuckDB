#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/execution/operator/filter/physical_use_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalUseBF &op) {
    unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
    auto create_bf = make_uniq<PhysicalUseBF>(plan->types, op.bf_to_use, op.estimated_cardinality);
    create_bf->children.emplace_back(std::move(plan));
    plan = std::move(create_bf);
    return plan;
}
}