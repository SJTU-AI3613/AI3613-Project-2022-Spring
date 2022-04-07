#pragma once

#include <memory>
#include <vector>

namespace naivedb {
namespace storage {
class Tuple;
}
namespace catalog {
class Schema;
}
namespace query {
class ExecutorContext;
}
}  // namespace naivedb

namespace naivedb::query {
/**
 * @brief Executor is the base class of all physical plan executors. It adopts the iterator model (also known as Volcano
 * model).
 *
 */
class Executor {
  public:
    /**
     * @brief Construct a new Executor object
     *
     * @param context the executor context that the executor runs with
     * @param children the children executors of this executor
     */
    explicit Executor(ExecutorContext &context, std::vector<std::unique_ptr<Executor>> &&children)
        : context_(context), children_(std::move(children)) {}

    /**
     * @brief Destroy the Executor object
     *
     */
    virtual ~Executor() = default;

    /**
     * @brief Initialize the executor and its children.
     * @note This method must be called before next() is called!
     */
    virtual void init() = 0;

    /**
     * @brief Yield one or group of tuples (for GroupByExecutor) from this executor.
     *
     * @return std::vector<storage::Tuple>
     */
    virtual std::vector<storage::Tuple> next() = 0;

    /**
     * @brief Get the schema of the tuples that this executor produces.
     *
     * @return const catalog::Schema*
     */
    virtual const catalog::Schema *output_schema() const = 0;

    /**
     * @brief Get the executor context.
     *
     * @return ExecutorContext*
     */
    ExecutorContext &context() const { return context_; }

    /**
     * @brief Get the child executor at index child_id.
     *
     * @param child_id
     * @return Executor*
     */
    Executor *child_at(size_t child_id) const { return children_[child_id].get(); }

    /**
     * @brief Get all the children executors.
     *
     * @return const std::vector<std::unique_ptr<Executor>>&
     */
    const std::vector<std::unique_ptr<Executor>> &children() const { return children_; }

  private:
    ExecutorContext &context_;
    std::vector<std::unique_ptr<Executor>> children_;
};
}  // namespace naivedb::query