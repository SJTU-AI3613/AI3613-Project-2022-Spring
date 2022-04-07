#pragma once

namespace naivedb {
namespace catalog {
class Catalog;
}
namespace buffer {
class BufferManager;
}
}  // namespace naivedb

namespace naivedb::query {
/**
 * @brief ExecutorContext stores all the context necessary to run an executor.
 *
 */
class ExecutorContext {
  public:
    ExecutorContext(buffer::BufferManager *buffer_manager, catalog::Catalog *catalog)
        : buffer_manager_(buffer_manager), catalog_(catalog) {}

    buffer::BufferManager *buffer_manager() { return buffer_manager_; }

    catalog::Catalog *catalog() { return catalog_; }

  private:
    buffer::BufferManager *buffer_manager_;
    catalog::Catalog *catalog_;
};
}  // namespace naivedb::query