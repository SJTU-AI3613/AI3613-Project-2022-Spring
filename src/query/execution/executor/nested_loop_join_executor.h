#pragma once

#include "common/utils.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_nested_loop_join.h"
#include "storage/tuple/tuple.h"

namespace naivedb::query {
class NestedLoopJoinExecutor : public Executor {
  public:
    NestedLoopJoinExecutor(ExecutorContext &context,
                           const PhysicalNestedLoopJoin *plan,
                           std::unique_ptr<Executor> &&left,
                           std::unique_ptr<Executor> &&right)
        : Executor(context, make_vector(std::move(left), std::move(right))), plan_(plan) {}

    virtual ~NestedLoopJoinExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalNestedLoopJoin *plan_;
    std::vector<storage::Tuple> left_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query