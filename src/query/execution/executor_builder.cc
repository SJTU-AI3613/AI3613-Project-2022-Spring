#include "query/execution/executor_builder.h"

#include "query/execution/executor/aggregate_executor.h"
#include "query/execution/executor/filter_executor.h"
#include "query/execution/executor/group_by_executor.h"
#include "query/execution/executor/hash_join_executor.h"
#include "query/execution/executor/insert_executor.h"
#include "query/execution/executor/nested_loop_join_executor.h"
#include "query/execution/executor/projection_executor.h"
#include "query/execution/executor/seq_scan_executor.h"
#include "query/execution/executor/update_executor.h"
#include "query/physical_plan/physical_aggregate.h"
#include "query/physical_plan/physical_filter.h"
#include "query/physical_plan/physical_group_by.h"
#include "query/physical_plan/physical_hash_join.h"
#include "query/physical_plan/physical_nested_loop_join.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
void ExecutorBuilder::Visitor::visit(const PhysicalHashJoin *plan) {
    plan->left_plan()->accept(*this);
    auto left = std::move(executor_);
    plan->right_plan()->accept(*this);
    auto right = std::move(executor_);
    executor_ = std::make_unique<HashJoinExecutor>(context_, plan, std::move(left), std::move(right));
}

void ExecutorBuilder::Visitor::visit(const PhysicalInsert *plan) {
    executor_ = std::make_unique<InsertExecutor>(context_, plan);
}

void ExecutorBuilder::Visitor::visit(const PhysicalNestedLoopJoin *plan) {
    plan->left_plan()->accept(*this);
    auto left = std::move(executor_);
    plan->right_plan()->accept(*this);
    auto right = std::move(executor_);
    executor_ = std::make_unique<NestedLoopJoinExecutor>(context_, plan, std::move(left), std::move(right));
}

void ExecutorBuilder::Visitor::visit(const PhysicalProjection *plan) {
    plan->child()->accept(*this);
    executor_ = std::make_unique<ProjectionExecutor>(context_, plan, std::move(executor_));
}

void ExecutorBuilder::Visitor::visit(const PhysicalSeqScan *plan) {
    executor_ = std::make_unique<SeqScanExecutor>(context_, plan);
}

void ExecutorBuilder::Visitor::visit(const PhysicalFilter *plan) {
    plan->child()->accept(*this);
    executor_ = std::make_unique<FilterExecutor>(context_, plan, std::move(executor_));
}

void ExecutorBuilder::Visitor::visit(const PhysicalGroupBy *plan) {
    plan->child()->accept(*this);
    executor_ = std::make_unique<GroupByExecutor>(context_, plan, std::move(executor_));
}

void ExecutorBuilder::Visitor::visit(const PhysicalAggregate *plan) {
    plan->child()->accept(*this);
    executor_ = std::make_unique<AggregateExecutor>(context_, plan, std::move(executor_));
}

void ExecutorBuilder::Visitor::visit(const PhysicalUpdate *plan) {
    executor_ = std::make_unique<UpdateExecutor>(context_, plan);
}

std::unique_ptr<Executor> ExecutorBuilder::build(const PhysicalPlan *plan) const {
    Visitor visitor(context_);
    plan->accept(visitor);
    return visitor.executor();
}
}  // namespace naivedb::query