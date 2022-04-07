#pragma once

#include "common/utils.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_projection.h"

namespace naivedb::query {
class ProjectionExecutor : public Executor {
  public:
    ProjectionExecutor(ExecutorContext &context, const PhysicalProjection *plan, std::unique_ptr<Executor> &&child)
        : Executor(context, make_vector(std::move(child))), plan_(plan) {}

    virtual ~ProjectionExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalProjection *plan_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query