#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {
unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateBF &op) {
    unique_ptr<PhysicalOperator> plan = CreatePlan(*op.children[0]);
    auto create_bf = make_uniq<PhysicalCreateBF>(plan->types, op.bf_to_create, op.estimated_cardinality,
                                                 TaskScheduler::GetScheduler(this->context).NumberOfThreads());
    create_bf->children.emplace_back(std::move(plan));
    plan = std::move(create_bf);
    return plan;
}
}