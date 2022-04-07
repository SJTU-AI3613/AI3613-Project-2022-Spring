#pragma once

#include "catalog/schema.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_update.h"
#include "storage/table/table_heap.h"

namespace naivedb::query {
class UpdateExecutor : public Executor {
  public:
    UpdateExecutor(ExecutorContext &context, const PhysicalUpdate *plan) : Executor(context, {}), plan_(plan) {}

    virtual ~UpdateExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalUpdate *plan_;
    std::unique_ptr<storage::TableHeap> table_heap_;
    const catalog::Schema *table_schema_;
    storage::TableHeap::Iterator table_iter_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query