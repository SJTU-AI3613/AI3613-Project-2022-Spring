#pragma once

namespace naivedb {
namespace query {
class PhysicalHashJoin;
class PhysicalInsert;
class PhysicalNestedLoopJoin;
class PhysicalProjection;
class PhysicalSeqScan;
class PhysicalFilter;
class PhysicalGroupBy;
class PhysicalAggregate;
class PhysicalUpdate;
}  // namespace query
}  // namespace naivedb

namespace naivedb::query {
/**
 * @brief PhysicalPlanVisitor is the abstract class for physical plan visitors.
 *
 */
class PhysicalPlanVisitor {
  public:
    virtual void visit(const PhysicalHashJoin *) = 0;

    virtual void visit(const PhysicalInsert *) = 0;

    virtual void visit(const PhysicalNestedLoopJoin *) = 0;

    virtual void visit(const PhysicalProjection *) = 0;

    virtual void visit(const PhysicalSeqScan *) = 0;

    virtual void visit(const PhysicalFilter *) = 0;

    virtual void visit(const PhysicalGroupBy *) = 0;

    virtual void visit(const PhysicalAggregate *) = 0;

    virtual void visit(const PhysicalUpdate *) = 0;
};
}  // namespace naivedb::query