#pragma once

#include "catalog/schema.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_group_by.h"
#include "type/value.h"

#include <unordered_map>

namespace naivedb::query {
class GroupByExecutor : public Executor {
  public:
    GroupByExecutor(ExecutorContext &context, const PhysicalGroupBy *plan, std::unique_ptr<Executor> &&child)
        : Executor(context, make_vector(std::move(child))), plan_(plan) {}

    virtual ~GroupByExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalGroupBy *plan_;
    std::unordered_map<type::Value, std::vector<storage::Tuple>> buckets_;
    decltype(buckets_)::iterator buckets_iter_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query