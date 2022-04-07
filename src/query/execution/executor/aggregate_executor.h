#pragma once

#include "common/utils.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_aggregate.h"

namespace naivedb::query {
class AggregateExecutor : public Executor {
  public:
    AggregateExecutor(ExecutorContext &context, const PhysicalAggregate *plan, std::unique_ptr<Executor> &&child)
        : Executor(context, make_vector(std::move(child))), plan_(plan) {}

    virtual ~AggregateExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalAggregate *plan_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query