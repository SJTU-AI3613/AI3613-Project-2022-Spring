#include "query/execution/executor/update_executor.h"

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "lock/lock_manager.h"
#include "storage/table/table_heap.h"
#include "storage/tuple/tuple.h"
#include "transaction/transaction.h"
#include "type/value.h"

namespace naivedb::query {
void UpdateExecutor::init() {
    // TODO(Project-1): Implement this method
    UNIMPLEMENTED;
}

std::vector<storage::Tuple> UpdateExecutor::next() {
    // TODO(Project-1): Implement this method
    UNIMPLEMENTED;
    return {};
}
}  // namespace naivedb::query