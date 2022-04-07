#pragma once

#include "common/utils.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_insert.h"

namespace naivedb {
namespace storage {
class TableHeap;
}
}  // namespace naivedb

namespace naivedb::query {
class InsertExecutor : public Executor {
  public:
    InsertExecutor(ExecutorContext &context, const PhysicalInsert *plan) : Executor(context, {}), plan_(plan) {}

    virtual ~InsertExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalInsert *plan_;
    std::unique_ptr<storage::TableHeap> table_heap_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query