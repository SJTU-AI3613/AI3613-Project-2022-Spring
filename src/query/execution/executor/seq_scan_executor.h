#pragma once

#include "catalog/schema.h"
#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_seq_scan.h"
#include "storage/table/table_heap.h"

namespace naivedb::query {
class SeqScanExecutor : public Executor {
  public:
    SeqScanExecutor(ExecutorContext &context, const PhysicalSeqScan *plan) : Executor(context, {}), plan_(plan) {}

    virtual ~SeqScanExecutor() = default;

    virtual void init() override;

    virtual std::vector<storage::Tuple> next() override;

    virtual const catalog::Schema *output_schema() const override { return plan_->output_schema(); }

  private:
    const PhysicalSeqScan *plan_;
    std::unique_ptr<storage::TableHeap> table_heap_;
    storage::TableHeap::Iterator table_iter_;
    // TODO(Project-1): Add more members if you need
};
}  // namespace naivedb::query