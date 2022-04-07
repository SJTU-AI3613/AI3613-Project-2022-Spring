#pragma once

#include "query/execution/executor/executor.h"
#include "query/execution/executor_context.h"
#include "query/physical_plan/physical_plan.h"
#include "query/physical_plan/physical_plan_visitor.h"

namespace naivedb::query {
class ExecutorBuilder {
  private:
    class Visitor : public PhysicalPlanVisitor {
      public:
        explicit Visitor(ExecutorContext &context) : context_(context) {}

        virtual void visit(const PhysicalHashJoin *plan) override;

        virtual void visit(const PhysicalInsert *plan) override;

        virtual void visit(const PhysicalNestedLoopJoin *plan) override;

        virtual void visit(const PhysicalProjection *plan) override;

        virtual void visit(const PhysicalSeqScan *plan) override;

        virtual void visit(const PhysicalFilter *plan) override;

        virtual void visit(const PhysicalGroupBy *plan) override;

        virtual void visit(const PhysicalAggregate *plan) override;

        virtual void visit(const PhysicalUpdate *plan) override;

        std::unique_ptr<Executor> executor() { return std::move(executor_); }

      private:
        ExecutorContext &context_;
        std::unique_ptr<Executor> executor_;
    };

  public:
    explicit ExecutorBuilder(ExecutorContext &context) : context_(context) {}

    std::unique_ptr<Executor> build(const PhysicalPlan *plan) const;

  private:
    ExecutorContext &context_;
};
}  // namespace naivedb::query