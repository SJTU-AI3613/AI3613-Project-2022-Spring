#pragma once

#include "common/utils.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_hash_join.h"
#include "storage/tuple/tuple.h"
#include "type/value.h"

#include <unordered_map>

namespace naivedb::query {
class HashJoinExecutor : public Executor {
  public:
    HashJoinExecutor(ExecutorContext &context,
                     const PhysicalHashJoin *plan,
                     std::unique_ptr<Executor> &&left,
                     std::unique_ptr<Executor> &&right)
        : Executor(context, make_vector(std::move(left), std::move(right))), plan_(plan) {}

    virtual ~HashJoinExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalHashJoin *plan_;
    std::unordered_map<type::Value, storage::Tuple> hash_table_;
    std::vector<storage::Tuple> right_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query